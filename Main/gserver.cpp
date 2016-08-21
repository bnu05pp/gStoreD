/*=============================================================================
# Filename: gserver.cpp
# Author: Bookug Lobert
# Mail: 1181955272@qq.com
# Last Modified: 2016-02-26 19:15
# Description: first written by hanshuo, modified by zengli
=============================================================================*/

#include "../Server/Server.h"
#include "../Util/Util.h"

using namespace std;

string pid_file_path(unsigned short);
bool save_pid_file(unsigned short);
bool remove_pid_file(unsigned short);
unsigned get_pid_from_file(unsigned short);
unsigned short get_port_from_string(const char*);
bool is_same_program(unsigned, unsigned);

int main(int argc, char * argv[])
{
#ifdef DEBUG
	Util util;
#endif
	unsigned short port = Socket::DEFAULT_CONNECT_PORT;

	if (argc > 1) {
		if (string(argv[1]) == "-s") { // to stop a server

			if (argc > 2) {
				port = get_port_from_string(argv[2]);

				if (port == 0) {
					cerr << "invalid port: " << argv[2] << endl;
					return -1;
				}
			}

			unsigned pid = get_pid_from_file(port);

			if (pid == 0) {
				cout << "no server running at port " << port << endl;
				return -1;
			}

			unsigned pid_this = (unsigned)getpid();

			if (!is_same_program(pid_this, pid)) {
				cout << "no server running at port " << port << endl;
				remove_pid_file(port);
				return -1;
			}

			// QUESTION: how to stop a server properly?
			string cmd = "kill " + Util::int2string(pid);
			int ret = system(cmd.c_str());

			if (ret == 0) {
				cout << "server stopped at port " << port << endl;
				remove_pid_file(port);
			}
			else {
				cerr << "failed to stop server at port " << port << endl;
			}

			return ret;
		}

		port = get_port_from_string(argv[1]);

		if (port == 0) {
			cerr << "invalid port: " << argv[1] << endl;
			return -1;
		}
	}

	if (access(pid_file_path(port).c_str(), F_OK) == 0) {
		unsigned pid_other = get_pid_from_file(port);

		if (pid_other == 0) {
			cerr << "cannot start server at port " << port << endl;
			return -1;
		}

		unsigned pid_this = (unsigned)getpid();

		if (is_same_program(pid_this, pid_other)) {
			cout << "gserver already running at port " << port << endl;
			return -1;
		}

		remove_pid_file(port);
	}

	if (!save_pid_file(port)) {
		cerr << "cannot start server at port " << port << endl;
		return -1;
	}

	Server server(port);

#ifdef DEBUG
	cout << "port=" << port << endl; //debug
#endif

	server.createConnection();
	server.listen();

	cout << "server started at port " << port << endl;
	return 0;
}

string pid_file_path(unsigned short port) {
	return Util::tmp_path + ".gserver_" + Util::int2string(port);
}

bool save_pid_file(unsigned short port) {
	ofstream pid_file(pid_file_path(port).c_str(), ios::out);

	if (!pid_file) {
		return false;
	}

	unsigned pid = (unsigned)getpid();
	pid_file << pid << endl;

	pid_file.close();
	return true;
}

bool remove_pid_file(unsigned short port) {
	return remove(pid_file_path(port).c_str()) == 0;
}

unsigned get_pid_from_file(unsigned short port) {
	ifstream pid_file(pid_file_path(port).c_str(), ios::out);

	if (!pid_file) {
		return 0;
	}

	unsigned pid;
	pid_file >> pid;

	pid_file.close();
	return pid;
}

unsigned short get_port_from_string(const char* str) {
	stringstream ss(str);
	int tmp;
	ss >> tmp;
	if (tmp <= 0 || tmp > 65535) {
		return 0;
	}
	return (unsigned short)tmp;
}

bool is_same_program(unsigned pid_first, unsigned pid_second) {
	string cmd_first = "readlink /proc/" + Util::int2string(pid_first) + "/exe";
	string cmd_second = "readlink /proc/" + Util::int2string(pid_second) + "/exe";
	string path_first = Util::getSystemOutput(cmd_first);
	string path_second = Util::getSystemOutput(cmd_second);

	return path_first == path_second;
}