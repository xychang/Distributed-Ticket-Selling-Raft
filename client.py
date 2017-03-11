#	client 
#	Implemented by Zhijing@Mar. 7
import json
import socket
import datetime
import sys
import time


CONFIG = json.load(open('config.json'))

client_id = sys.argv[1]

def Request(port, buy_num, request_id):
	c = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	host = ''
	addr = (host, port)
	message = ('BUY:"{client_id}",{request_id},' +
               '{ticket_count}').format(
                           client_id=client_id,
                           request_id=request_id,
                           ticket_count=buy_num)
	sent = c.sendto(message, addr)
	time.sleep(2)
	data, server = c.recvfrom(4096)
	c.close()



def Interface_cmd():
	choice = True
	request_id = 0
	while choice:
		datacenter = CONFIG['datacenters']
		datacenter_list = []
		for i in range(1, len(datacenter)+1):
			datacenter_list.append(datacenter[str(i)]['port'])
			print('datacenter: '+ str(datacenter[str(i)]['port']))

		cmd = raw_input('Please choose a server to connect... or Press N to exit...\t')
		if cmd == 'N':
			choice = False
			break
		else:
			server_selected = int(cmd)
			if server_selected not in datacenter_list:
				print('Invalid input')
				continue

			buy_num = raw_input('Command: Buy/ Change/ Show?\t')
			

			Request(server_selected, buy_num, request_id)
			request_id += 1
			


def main():
	print("\n*********************************\n")
	print("Welcome to SANDLAB Ticket Office!")
	print("The current time is " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
	print("\n*********************************\n")
	Interface_cmd()
	exit()

if __name__ == "__main__":
	main()

