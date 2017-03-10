#	client 
#	Implemented by Zhijing@Mar. 7
import json
import socket
import datetime
import sys
import time


CONFIG = json.load(open('config.json'))

def Request(port, buy_num):
	c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	host = ''
	c.connect((host, port))
	c.sendall(str(buy_num))
	time.sleep(2)
	print c.recv(1024)
	c.close()



def Interface_cmd():
	choice = True
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
			

			Request(server_selected, buy_num)
			


def main():
	print("\n*********************************\n")
	print("Welcome to SANDLAB Ticket Office!")
	print("The current time is " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
	print("\n*********************************\n")
	Interface_cmd()
	exit()

if __name__ == "__main__":
	main()

