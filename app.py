import eventlet
import socketio

# Initialize Socket.io server
sio = socketio.Server(cors_allowed_origins='*')
app = socketio.WSGIApp(sio)

# Room storage: { room_id: { host_id, movie_url, users: [] } }
rooms = {}

@sio.event
def connect(sid, environ):
    print(f"User connected: {sid}")

@sio.on('create-room')
def on_create(sid, data):
    room_id = data['roomId']
    host_name = data['hostName']
    movie_url = data['movieUrl']
    
    sio.enter_room(sid, room_id)
    rooms[room_id] = {
        'host': sid,
        'movieUrl': movie_url,
        'users': [{'id': sid, 'name': host_name}]
    }
    sio.emit('room-created', room_id, room=sid)
    print(f"Room {room_id} created by {host_name}")

@sio.on('join-room')
def on_join(sid, data):
    room_id = data['roomId']
    user_name = data['userName']
    
    if room_id in rooms:
        sio.enter_room(sid, room_id)
        rooms[room_id]['users'].append({'id': sid, 'name': user_name})
        # Notify room of updated user list
        sio.emit('user-list', rooms[room_id]['users'], room=room_id)
        # Send current movie info to joiner
        sio.emit('joined-room', {'movieUrl': rooms[room_id]['movieUrl']}, room=sid)
    else:
        sio.emit('error', 'Room not found', room=sid)

@sio.on('sync-video')
def on_sync(sid, data):
    room_id = data['roomId']
    if room_id in rooms and rooms[room_id]['host'] == sid:
        # Broadcast host's state to everyone else in the room
        sio.emit('video-update', {
            'state': data['state'],
            'time': data['time']
        }, room=room_id, skip_sid=sid)

@sio.on('stop-party')
def on_stop(sid, room_id):
    if room_id in rooms and rooms[room_id]['host'] == sid:
        sio.emit('party-ended', room=room_id)
        del rooms[room_id]

@sio.event
def disconnect(sid):
    print(f"User disconnected: {sid}")

if __name__ == '__main__':
    eventlet.wsgi.server(eventlet.listen(('', 3000)), app)
