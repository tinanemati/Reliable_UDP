#! python3
# server_putah.py
# usage: python3 server_putah.py --ip XXXX.XXXX.XXXX.XXXX --port YYYY

import socket
import threading
import argparse
import zlib
import struct
import os
import time
from datetime import datetime


def timestamp():
    timestamp = time.time()
    return datetime.utcfromtimestamp(timestamp).strftime('%Y%m%d%H%M%S')


def file_logging(msg):
    write_file = open(log_file_path, 'a')
    write_file.write(msg + '\n')
    write_file.close()
    print(msg)


class ClientThread(threading.Thread):
    def __init__(self, clientAddress, clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        self.caddress = clientAddress
        print ("New connection added: ", clientAddress)
    def run(self):
        print("Connection from : ", self.caddress)
        while self.csocket:
            try:
                listen(self.csocket)
            except:
                break
        print("Client at ", self.caddress, " disconnected...")
        self.csocket.close()

log_file = f"server_putah_{timestamp()}.log"
log_file_path = os.path.abspath(os.path.join('.', log_file))

# create argument parser
parser = argparse.ArgumentParser()

# add required server_ip argument
parser.add_argument('-s', '--ip', type=str, required=True)

# add the required server_port argument
parser.add_argument('-p', '--port', type=int, required=True)

# parse the arguments
args = parser.parse_args()

# ip address or hostname of the hose
host = args.ip

# port the host is listening for connections on
port = args.port

# message to be sent to server
message = "Pong"

# receive 16 bytes each time
BUFFER_SIZE = 1024

# create socket to listen on
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# bind the socket to the SERVER_HOST and SERVER_PORT
s.bind((host, port))

# checksum calculator for ensuring our data is being transferred
def checksum_calc(data):
    checksum = zlib.crc32(data)
    return checksum


def accept(sock):
    r_header, r_data, r_address = receive(sock, buffer=BUFFER_SIZE)
    tcp_port = find_open_ports()
    m = ''
    data = m.encode()
    if r_header[7] and not r_header[6] and not r_header[8]:  # initial welcome handshake received
        accept_handshake_header_p, accept_handshake_header = header(tcp_port, r_address[1], data, r_header[4],
                                         ack_seq=r_header[4] + 1, ack=True, syn=True)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            send(s, accept_handshake_header_p + data, (r_address[0], r_header[0]))
        return accept(sock)
    elif not r_header[7] and r_header[6] and not r_header[8]:
        print(f"Connection established with {(r_address[0], r_header[0])}")
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tcp_sock.bind((host, tcp_port))
        print(f"binding listener to {host}:{tcp_port}")
        return tcp_sock, (r_address[0], r_header[0])
    else:
        print("did not receive ack")


def listen(sock, listen_seq=0):
    r_header, r_data, r_address = receive(sock, buffer=BUFFER_SIZE)
    send_port = r_header[0]
    rec_port = r_header[1]
    win = r_header[2]
    checksum = r_header[3]
    seq = r_header[4]
    ack_seq = r_header[5]
    ack = r_header[6]
    syn = r_header[7]
    fin = r_header[8]
    rec_checksum = checksum_calc(r_data)
    print(f"checksum: {checksum} \nrec_checksum: {rec_checksum}")
    if checksum == rec_checksum:  # data received, not corrupted
        if fin:
            close(sock, (r_address[0], send_port), ack=True)
            print("Connection closed")
        elif ack and ack_seq > listen_seq:
            r_message = r_data.decode()
            print(f"received: {r_message} from {r_address}")
            data = message.encode()
            reply_header_p, reply_header = header(rec_port, send_port, data, seq, ack_seq=ack_seq + 1, ack=True)
            print(f"responding: {message} to ('{r_address[0]}',{send_port})")
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                send(s, reply_header_p + data, (r_address[0], send_port))
    else:  # data received is corrupted, request the message again
        print("data corrupted or not received")
        data = ''.encode()
        reply_header_p, reply_header = header(rec_port, send_port, data, seq, ack_seq=ack_seq, ack=True)
        send(sock, reply_header_p + data, (r_address[0], send_port))


def close(s, address, tries=0, ack=False):
    print(f"Disconnecting from: {address}")
    if s:
        s.settimeout(5)
        if tries < 3:  # wait for acknowledgement from client before closing
            data = ''.encode()
            close_header_p, close_header = header(s.getsockname()[1], address[1], data,
                                                  0, ack=ack, fin=True)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                send(s, close_header_p + data, address)
            if ack:
                # sent acknowledgment, no need to wait for response
                s.close()
            else:
                try:
                    r_header, r_data, r_address = receive(s)
                    if r_header[6] and r_header[8]:
                        print("Client responded to disconnect.")
                        s.close()
                    else:
                        close(s, address, tries + 1)
                except:
                    close(s, address, tries + 1)
        else:  # server not responding, close anyway
            s.close()


def send(s, packet, address):
    s.sendto(packet, address)
    header = packet[:27]
    header = unpack_header(header)
    file_logging(f"{header[0]} | {header[1]} | {msg_type(header)} | {header[2]} | {timestamp()}")



def receive(rec_sock, buffer=1024):
    r, r_address = rec_sock.recvfrom(buffer)
    r_header = r[:27]
    r_header = unpack_header(r_header)
    data = r[27:]
    file_logging(f"{r_header[0]} | {r_header[1]} | {msg_type(r_header)} | {r_header[2]} | {timestamp()}")
    return r_header, data, r_address


def header(sender_port, receiver_port, data, seq, ack_seq=0, ack=False, syn=False, fin=False):
    # create the header ('struct format', 'sender port', 'receiver port', 'win',
    # 'checksum', 'seq', 'ack seq', 'ack', 'syn', 'fin')
    win = len(data)
    checksum = checksum_calc(data)
    if ack_seq == 0:
        ack_seq = seq
    header_packed = struct.pack("!IIIIII???", sender_port, receiver_port, win, checksum, seq,
                       ack_seq, ack, syn, fin)
    return header_packed, unpack_header(header_packed)


def unpack_header(header):
    return struct.unpack("!IIIIII???", header)


def find_open_ports():
    for p in range(7001, 8000):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            res = s.connect_ex((host, p))
            if res == 0:
                return p


def msg_type(header):
    if header[2] > 0:
        return "DATA"
    elif header[6] and header[7]:
        return "ACK/SYN"
    elif header[6]:
        return "ACK"
    elif header[7]:
        return "SYN"
    elif header[8]:
        return "FIN"


while True:
    try:
        print("Listening as " + host + ":" + str(port))
        client_socket, address = accept(s)
        try:
            newthread = ClientThread(address, client_socket)
            newthread.start()
        except KeyboardInterrupt:
            print("Keyboard Interrupt, exiting and closing connections.")
            close(client_socket, address)
            break
    except KeyboardInterrupt:
        print("Keyboard Interrupt, closing server.")
        break

# close the server socket
s.close()
