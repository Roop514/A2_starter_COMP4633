import json
from concurrent import futures

import grpc
import mysql.connector
import event_pb2 as pb
import event_pb2_grpc as rpc

CONFIG = json.load(open("config.json"))


def read(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor(dictionary=True) as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall()


def write(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor() as cursor:
        cursor.execute(sql, args)
        db.commit()
        return cursor.lastrowid


class EventService(rpc.EventServiceServicer):
    def ListEvents(self, request, context):
        if request.name_querry:
            found = read("""SELECT event_id, name, capacity, capacity - seats_taken AS available_seats
                        FROM events WHERE name LIKE %s ORDER BY event_id""", ("%" + request.name_querry + "%",))
        else:
            found = read("""SELECT event_id, name, capacity, capacity - seats_taken AS available_seats
                        FROM events ORDER BY event_id""", ())
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Event not found")
        return pb.ListEventsResponse(events = [pb.Event(**event) for event in found])

    def GetEvent(self, request, context):
        found = read("""SELECT event_id, name, capacity, capacity - seats_taken AS available_seats
                        FROM events WHERE event_id = %s""", (request.event_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Event not found")
        return pb.Event(**found[0])

    def ReserveSeat(self, request, context):
        # Check that the event exists
        self.GetEvent(request, context)

        with mysql.connector.connect(**CONFIG["db"]) as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """UPDATE events
                       SET seats_taken = seats_taken + 1
                       WHERE event_id = %s
                       AND seats_taken < capacity""",
                    (request.event_id,)
                )
                db.commit()

                if cursor.rowcount == 0:
                    context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        "Event is full"
                    )

        return pb.Empty()

    def ReleaseSeat(self, request, context):
        self.GetEvent(request, context)
        write("UPDATE events SET seats_taken = seats_taken - 1 WHERE event_id = %s", (request.event_id,))
        return pb.Empty()

    def EditEvent(self, request, context):
        event = self.GetEvent(request, context)

        if request.HasField("capacity"):
            seats_taken = event.capacity - event.available_seats
            if request.capacity < seats_taken:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Capacity cannot be less than seats already booked")
        if request.HasField("name"):
            updated_name = request.name.strip()
            if updated_name == "":
                 context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Event name cannot be blank")

        if request.HasField("name") and request.HasField("capacity"):
            write("""UPDATE events 
                 SET name = %s, capacity = %s
                 WHERE event_id = %s""",(updated_name, request.capacity, request.event_id,)) 

        elif request.HasField("name"):
            write("""UPDATE events
                 SET name = %s
                 WHERE event_id = %s""", (updated_name, request.event_id))

        elif request.HasField("capacity"):
            write("""UPDATE events
                 SET capacity = %s
                 WHERE event_id = %s""", (request.capacity, request.event_id))

        updated_event = self.GetEvent(request, context)
        return pb.UpdateEventResponse(event = updated_event)

server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
rpc.add_EventServiceServicer_to_server(EventService(), server)
server.add_insecure_port(CONFIG["listen"])
server.start()
print(f"Event service on {CONFIG['listen']}", flush=True)
server.wait_for_termination()
