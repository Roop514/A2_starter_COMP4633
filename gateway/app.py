import json

import grpc
from flask import Flask, abort, jsonify, request
from werkzeug.exceptions import HTTPException
import event_pb2
import event_pb2_grpc
import registration_pb2
import registration_pb2_grpc

CONFIG = json.load(open("config.json"))
STATUS = {
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
    grpc.StatusCode.FAILED_PRECONDITION: 409,
    grpc.StatusCode.UNIMPLEMENTED: 501,
    grpc.StatusCode.UNAVAILABLE: 503,
}

app = Flask(__name__)
events = event_pb2_grpc.EventServiceStub(grpc.insecure_channel(CONFIG["event_service"]))
registrations = registration_pb2_grpc.RegistrationServiceStub(
    grpc.insecure_channel(CONFIG["registration_service"]))

@app.before_request
def check_staff_auth():
    protected = (request.method == "POST" and request.path == ("/api/v1/events") or
    request.method in ("PATCH", "DELETE") and request.path.startswith("/api/v1/events/")
    )
    if protected:
        authorization = request.headers.get("Authorization")
        expected = f"Bearer {CONFIG['staff_token']}"
        if expected != authorization:
          return jsonify(error="Unauthorized"), 401

def fields(message):
    return {field.name: getattr(message, field.name) for field in message.DESCRIPTOR.fields}


@app.errorhandler(grpc.RpcError)
def rpc_error(error):
    return jsonify(error=error.details()), STATUS.get(error.code(), 500)


@app.errorhandler(HTTPException)
def http_error(error):
    return jsonify(error=error.description), error.code


@app.get("/api/v1/events")
def list_events():
    name = request.args.get('name')
    if name:
        response = events.ListEvents(event_pb2.ListEventsRequest(name_querry=name))
    else:
        response = events.ListEvents(event_pb2.ListEventsRequest())
    return jsonify([fields(event) for event in response.events]), 200



@app.get("/api/v1/events/<int:event_id>")
def get_event(event_id):
    return jsonify(fields(events.GetEvent(event_pb2.EventId(event_id=event_id))))


@app.post("/api/v1/events")
def add_event():
    if not request.is_json:
        return jsonify(error="Request body must be JSON"), 400    
    body = request.get_json() 
    if not isinstance(body, dict):
        return jsonify(error="Request body must be JSON object"), 400
    if not body:
        return jsonify(error="Request body cannot be empty"), 400
    required_fields = {"name", "capacity"}
    if not set(body.keys()).issubset(required_fields):
        return jsonify(error="Invalid Fields"), 400
    if "name" not in body:
        return jsonify(error="Name is required"), 400
    elif not isinstance(body["name"], str):
        return jsonify(error="Name must be a string"), 400
    if "capacity" not in body:
        return jsonify(error="Capacity is required"), 400
    elif not isinstance(body["capacity"], int) or body["capacity"] < 0:
        return jsonify(error="Capacity must be a non-negative integer"), 400 
    post_request = event_pb2.CreateBookingRequest(name=body["name"], capacity=body["capacity"])
    create_event_resp = events.CreateEvent(post_request)
    return jsonify(fields(create_event_resp.event))
    
    
    

@app.patch("/api/v1/events/<int:event_id>")
def edit_event(event_id):
    if not request.is_json:
        return jsonify(error="Request body must be JSON"), 400    
    body = request.get_json() 
    if not isinstance(body, dict):
        return jsonify(error="Request body must be JSON object"), 400
    if not body:
        return jsonify(error="Request body cannot be empty"), 400
    allowed_fields = {"name", "capacity"}
    if not set(body.keys()).issubset(allowed_fields):
        return jsonify(error="Invalid Fields"), 400
    if "name" in body:
        if not isinstance(body["name"], str):
            return jsonify(error="Name must be a string"), 400
    if "capacity" in body:
        if not isinstance(body["capacity"], int) or body["capacity"] < 0 or isinstance(body["capacity"],bool):
            return jsonify(error="Capacity must be a non-negative integer"), 400 
    update_request = event_pb2.UpdateEventRequest(event_id=event_id)
    if "name" in body:
        update_request.name = body["name"]
    if "capacity" in body:
        update_request.capacity = body["capacity"]
    update_response = events.EditEvent(update_request) 
    return jsonify(fields(update_response.event))


@app.delete("/api/v1/events/<int:event_id>")
def delete_event(event_id):
    abort(501, "TODO task 2: implement DELETE /api/v1/events/<event_id>")


@app.post("/api/v1/participants")
def add_participant():
    abort(501, "TODO task 3: implement POST /api/v1/participants")


@app.get("/api/v1/participants/<int:participant_id>")
def get_participant(participant_id):
    abort(501, "TODO task 3: implement GET /api/v1/participants/<participant_id>")


@app.get("/api/v1/events/<int:event_id>/registrations")
def list_bookings(event_id):
    abort(501, "TODO task 4: implement GET /api/v1/events/<event_id>/registrations")


@app.post("/api/v1/registrations")
def book():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        abort(400, "A JSON request body is required")

    event_id = data.get("event_id")
    participant_id = data.get("participant_id")

    if type(event_id) is not int or event_id <= 0:
        abort(400, "event_id must be a positive integer")

    if type(participant_id) is not int or participant_id <= 0:
        abort(400, "participant_id must be a positive integer")

    result = registrations.CreateBooking(
        registration_pb2.CreateBookingRequest(
            event_id=event_id,
            participant_id=participant_id
        )
    )

    return jsonify(fields(result)), 201

@app.delete("/api/v1/registrations/<int:booking_id>")
def cancel(booking_id):
    registrations.CancelBooking(registration_pb2.BookingId(booking_id=booking_id))
    return "", 204


app.run(host=CONFIG["host"], port=CONFIG["port"])
