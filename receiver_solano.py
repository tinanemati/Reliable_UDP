#! python3
# receiver_solano.py
# usage: python3 receiver_solano.py --ip XXXX.XXXX.XXXX.XXXX --port YYYY
# --packet_loss_percentage X --round_trip_jitter Y --output output.txt

import socket
import threading
import argparse
import zlib
import struct
import os
import time
import random
from datetime import datetime


def timestamp():
    timestamp = time.time()
    return datetime.utcfromtimestamp(timestamp).strftime('%Y%m%d%H%M%S')


def file_logging(msg):
    write_file = open(log_file_path, 'a')
    write_file.write(msg + '\n')
    write_file.close()
    # print(msg)


class ClientThread(threading.Thread):
    def __init__(self, clientaddress, clientsocket, seq):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        self.caddress = clientaddress
        self.seq = seq
        print("New connection added: ", clientaddress)

    def run(self):
        print("Connection from : ", self.caddress)
        os.makedirs(os.path.abspath(os.path.join('.', 'out_files', str(self.caddress[1])
                                                 )), exist_ok=True)
        output_file = os.path.abspath(os.path.join('.', 'out_files', str(self.caddress[1]
                                                                         ), args.output))
        while self.csocket:
            try:
                data, self.seq = listen(self.csocket, listen_seq=self.seq)
                with open(output_file, 'ab') as f:
                    f.write(data)
            except:
                break
        print("Client at ", self.caddress, " disconnected...")
        self.csocket.close()


log_file = f"receiver_solano_{timestamp()}.log"
log_file_path = os.path.abspath(os.path.join('.', log_file))

# create argument parser
parser = argparse.ArgumentParser()

# add required server_ip argument
parser.add_argument('-s', '--ip', type=str, required=True)

# add the required server_port argument
parser.add_argument('-p', '--port', type=int, required=True)

# add the required packet_loss_percentage argument
parser.add_argument('-l', '--packet_loss_percentage', type=int, default=10)

# add the required round_trip_jitter argument
parser.add_argument('-j', '--round_trip_jitter', type=float, default=0.5)

# add the required output argument
parser.add_argument('-o', '--output', type=str, required=True)

# parse the arguments
args = parser.parse_args()

# ip address or hostname of the hose
host = args.ip

# port the host is listening for connections on
port = args.port

# the amount of round trip jitter
jitter = args.round_trip_jitter

# the packet loss percentage
packet_loss = args.packet_loss_percentage

# receive 16 bytes each time
BUFFER_SIZE = 1000

# create socket to listen on
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# bind the socket to the SERVER_HOST and SERVER_PORT
s.bind((host, port))

SEQ = 0


def round_trip_jitter():
    j = random.randrange(0, 100)
    if j <= jitter:
        return True


def ploss():
    p = random.uniform(0, 1)
    if p > packet_loss:
        return p
    else:
        return 0


# checksum calculator for ensuring our data is being transferred
def checksum_calc(data):
    checksum = zlib.crc32(data)
    return checksum


def accept(sock, tcp=None):
    r_header, r_data, r_address = receive(sock, buffer=BUFFER_SIZE)
    tcp_port = 0
    data = ''.encode()
    if r_header[7] and not r_header[6] and not r_header[8]:  # initial welcome handshake received
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tcp_sock.bind((host, tcp_port))
        print(f"binding listener to {host}:{tcp_sock.getsockname()[1]}")
        accept_handshake_header_p, accept_handshake_header = header(tcp_sock.getsockname()[1],
                                                                    r_address[1], data, r_header[4],
                                                                    ack_seq=r_header[4] + 1, ack=True, syn=True)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            send(s, accept_handshake_header_p + data, (r_address[0], r_header[0]))
        return accept(sock, tcp_sock)
    elif not r_header[7] and r_header[6] and not r_header[8]:
        print(f"Connection established with {(r_address[0], r_header[0])}")
        return tcp, (r_address[0], r_header[0])
    else:
        print("did not receive handshake")


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
    # print(f"checksum: {checksum} \nrec_checksum: {rec_checksum}")
    if checksum == rec_checksum:  # data received, not corrupted
        if fin:
            close(sock, (r_address[0], send_port), ack=True)
        elif ack and ack_seq != listen_seq:
            r_message = r_data.decode()
            # print(f"received: {r_message} from {r_address}")
            data = ''.encode()
            reply_header_p, reply_header = header(rec_port, send_port, data, seq, ack_seq=listen_seq + win, ack=True)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                send(s, reply_header_p + data, (r_address[0], send_port))
        return r_data, listen_seq + win
    else:  # data received is corrupted, request the message again
        print("data corrupted or not received")
        data = ''.encode()
        reply_header_p, reply_header = header(rec_port, send_port, data, seq, ack_seq=listen_seq, ack=True)
        send(sock, reply_header_p + data, (r_address[0], send_port))
        return data, listen_seq


def close(sock, address, tries=0, ack=False):
    print(f"Disconnecting from: {address}")
    if sock:
        sock.settimeout(5)
        if tries < 3:  # wait for acknowledgement from client before closing
            data = ''.encode()
            close_header_p, close_header = header(sock.getsockname()[1], address[1], data,
                                                  0, ack=ack, fin=True)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                send(s, close_header_p + data, address)
            if ack:
                # sent acknowledgment, no need to wait for response
                s.close()
            else:
                try:
                    r_header, r_data, r_address = receive(sock)
                    if r_header[6] and r_header[8]:
                        print("Client responded to disconnect.")
                        s.close()
                    else:
                        close(sock, address, tries + 1)
                except:
                    close(sock, address, tries + 1)
        else:  # server not responding, close anyway
            sock.close()


def send(s, packet, address):
    sleepy_time = ploss()
    time.sleep(sleepy_time)  # sleep the receiver for the value of packet loss percentage in seconds
    s.sendto(packet, address)
    header = packet[:27]
    header = unpack_header(header)
    file_logging(f"{header[0]} | {header[1]} | {msg_type(header)} | {header[2]} | {timestamp()}")


def receive(rec_sock, buffer=BUFFER_SIZE):
    r, r_address = rec_sock.recvfrom(buffer)
    if not round_trip_jitter():  # if there is no jitter continue
        r_header = r[:27]
        r_header = unpack_header(r_header)
        data = r[27:]
        file_logging(f"{r_header[0]} | {r_header[1]} | {msg_type(r_header)} | {r_header[2]} | {timestamp()}")
        return r_header, data, r_address
    else:  # there was jitter, pretend like we did not receive the package
        return receive(rec_sock, buffer)


def header(sender_port, receiver_port, data, seq, ack_seq=0, ack=False, syn=False, fin=False):
    # create the header ('struct format', 'sender port', 'receiver port', 'win',
    # 'checksum', 'seq', 'ack seq', 'ack', 'syn', 'fin')
    win = len(data) + 27
    checksum = checksum_calc(data)
    if ack_seq == 0:
        ack_seq = seq
    header_packed = struct.pack("!IIIIII???", sender_port, receiver_port, win, checksum, seq,
                                ack_seq, ack, syn, fin)
    return header_packed, unpack_header(header_packed)


def unpack_header(header):
    return struct.unpack("!IIIIII???", header)


def msg_type(header):
    if header[2] > 27:
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
        ClientThread(address, client_socket, SEQ).start()
    except KeyboardInterrupt:
        print("Keyboard Interrupt, closing server.")
        break

# close the server socket
s.close()
