/*=============================================================================
# Filename: gload.cpp
# Author: Bookug Lobert 
# Mail: 1181955272@qq.com
# Last Modified: 2015-10-24 19:27
# Description: firstly written by liyouhuan, modified by zengli
TODO: add -h/--help for help message
=============================================================================*/

#include "../Util/Util.h"
#include "../Database/Database.h"
#include <mpi.h>

using namespace std;

vector<string> split(string textline, string tag){
	vector<string> res;
	std::size_t pre_pos = 0;
	std::size_t pos = textline.find_first_of(tag);
	while (pos != std::string::npos){
		string curStr = textline.substr(pre_pos, pos - pre_pos);
		curStr.erase(0, curStr.find_first_not_of("\r\t\n "));
		curStr.erase(curStr.find_last_not_of("\r\t\n ") + 1);
		if(strcmp(curStr.c_str(), "") != 0)
			res.push_back(curStr);
		pre_pos = pos + 1;
		pos = textline.find_first_of(tag, pre_pos);
	}

	string curStr = textline.substr(pre_pos, pos - pre_pos);
	curStr.erase(0, curStr.find_first_not_of("\r\t\n "));
	curStr.erase(curStr.find_last_not_of("\r\t\n ") + 1);
	if(strcmp(curStr.c_str(), "") != 0)
		res.push_back(curStr);

	return res;
}

/*
 * [0]./gload [1]data_folder_path  [2]rdf_file_path
 */
int 
main(int argc, char * argv[])
{
#ifdef DEBUG
	Util util;
#endif

	int myRank, p, size, i, j, k, l = 0;
	double partialResStart, partialResEnd;
	double schedulingStart, schedulingEnd;
	MPI_Status status;
	MPI_Init(&argc, &argv);

	MPI_Comm_rank(MPI_COMM_WORLD,&myRank);
	MPI_Comm_size(MPI_COMM_WORLD,&p);
	
	if(myRank == 0) {
		string _db_path = string(argv[1]);
		printf("The name of data store: %s!\n", argv[1]); 

		/*
		char* _config_name = new char[128];
		strcpy(_config_name, _db_path.c_str());
		size = strlen(_config_name);
		printf("DB_store: %s\n", _config_name);
		for(i = 1; i < p; i++){
			MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
			MPI_Send(_config_name, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
		}
		
		strcpy(_config_name, _rdf.c_str());
		size = strlen(_config_name);
		printf("RDF_data: %s\n", _config_name);
		for(i = 1; i < p; i++){
			MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
			MPI_Send(_config_name, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
		}
		
		strcpy(_config_name, _internal_vertices_file_path.c_str());
		size = strlen(_config_name);
		printf("Internal_vertices_file: %s\n", _config_name);
		for(i = 1; i < p; i++){
			MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
			MPI_Send(_config_name, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
		}
		*/
		stringstream* _six_tuples_ss = new stringstream[p - 1];		
		
		//add internal node flags to B+ tree
		Tree* tp = new Tree(".", string("internal_nodes.dat"), "build");
		string buff;
		string _internal_vertices_file = string(argv[3]);
		ifstream infile(_internal_vertices_file.c_str());
		if(!infile){
			cout << "import internal vertices failed." << endl;
		}
		
		while(getline(infile, buff)){
			vector<string> resVec = split(buff, "\t");
			char* _val = new char[resVec[1].size()];
			strcpy(_val, resVec[1].c_str());
			tp->insert(resVec[0].c_str(), strlen(resVec[0].c_str()), _val, strlen(_val));
			
			int partition_id = atoi(resVec[1].c_str());
			_six_tuples_ss[partition_id] << resVec[0] << endl;
		}

		infile.close();
		tp->save();
		
		for(i = 1; i < p; i++){
			char* _internal_vertices_arr = new char[_six_tuples_ss[i - 1].str().size() + 1];
			strcpy(_internal_vertices_arr, _six_tuples_ss[i - 1].str().c_str());
			size = strlen(_internal_vertices_arr);
			
			MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
			MPI_Send(_internal_vertices_arr, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
			
			delete[] _internal_vertices_arr;
		}		
		
		printf("The internal vertices have been sent!\n");
		
		ifstream _fin(argv[2]);
		if(!_fin) {
			cerr << "Fail to open the RDF data file: " << argv[2] << endl;
			exit(0);
		}
		
		RDFParser _parser(_fin);
		TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];
		
		int _sub_partition_id, _obj_partition_id;
		bool ret = true, ret1 = true;
		while(true)
		{
			int parse_triple_num = 0;

			_parser.parseFile(triple_array, parse_triple_num);

			//printf("parse %d triples!\n", parse_triple_num);
			if(parse_triple_num == 0) {
				for(i = 1; i < p; i++){
					char* _finished_tag = new char[32];
					strcpy(_finished_tag, "all tuples have sent ");
					size = strlen(_finished_tag);
					
					MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
					MPI_Send(_finished_tag, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
					
					delete[] _finished_tag;
				}
				break;
			}
			
			for(int i = 0; i < p - 1; i++){
				_six_tuples_ss[i].str("");
			}

			/* Process the Triple one by one */
			for(int i = 0; i < parse_triple_num; i ++)
			{
				
				string _sub = triple_array[i].getSubject();
				string _pre = triple_array[i].getPredicate();
				string _obj = triple_array[i].getObject();
				
				char* val = NULL;
				int vlen = 0;
				ret = tp->search(_sub.c_str(), strlen(_sub.c_str()), val, vlen);
				if(ret == true){
					_sub_partition_id = atoi(val);
					
					_six_tuples_ss[_sub_partition_id] << _sub << '\t' << _pre << '\t' << _obj << "." << endl;
				}
				// obj is entity
				if (triple_array[i].isObjEntity())
				{
					char* val1 = NULL;
					int vlen1 = 0;
					ret1 = tp->search(_obj.c_str(), strlen(_obj.c_str()), val1, vlen1);
					if(ret1 == true){
						_obj_partition_id = atoi(val1);
						
						if(_sub_partition_id != _obj_partition_id){
							_six_tuples_ss[_obj_partition_id] << _sub	<< '\t' << _pre << '\t' << _obj << "." << endl;
						}
					}
				}
				
				if(ret == false && ret1 == false){
					for(j = 1; j < p; j++){
						_six_tuples_ss[j - 1] << _sub << '\t' << _pre << '\t' << _obj << "." << endl;
					}
				}
			}
			
			for(i = 1; i < p; i++){
				char* _tuple_arr = new char[_six_tuples_ss[i - 1].str().size() + 1];
				strcpy(_tuple_arr, _six_tuples_ss[i - 1].str().c_str());
				size = strlen(_tuple_arr);
				
				//printf("after parsing %d triples, partition %d has size %d and there are : %s\n", parse_triple_num, i, size, _tuple_arr);
				
				MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
				MPI_Send(_tuple_arr, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
				
				delete[] _tuple_arr;
			}
		}

		delete[] triple_array;
		//delete[] _config_name;
		_fin.close();
		infile.close();
		delete tp;

	}else{
	/*
		MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
		char* _db_store_name = new char[size];
		MPI_Recv(_db_store_name, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
		_db_store_name[size - 1] = 0;
		
		MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
		char* _rdf_file_name = new char[size];
		MPI_Recv(_rdf_file_name, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
		_rdf_file_name[size - 1] = 0;
		
		MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
		char* _internal_vertices_file = new char[size];
		MPI_Recv(_internal_vertices_file, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
		_internal_vertices_file[size - 1] = 0;
		*/
		
		// recieve all internal vertices
		
		MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
		char* _internal_vertices_arr = new char[size];
		MPI_Recv(_internal_vertices_arr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
		_internal_vertices_arr[size - 1] = 0;
		
		ofstream _internal_vertices_fout("_distributed_gStore_tmp_internal_vertices.txt");
		_internal_vertices_fout << _internal_vertices_arr;
		_internal_vertices_fout.close();
		
		// recieve all triples
		
		string _db_path = string(argv[1]);
		Database _db(_db_path);
		
		ofstream _six_tuples_fout("_distributed_gStore_tmp_rdf_triples.n3");
		if(! _six_tuples_fout) {
			cerr << "Fail to open: _distributed_gStore_tmp_rdf_triples.n3" << endl;
			exit(0);
		}
		
		while(true){
			MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
			char* _tuples = new char[size];
			MPI_Recv(_tuples, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
			_tuples[size - 1] = 0;
			
			//printf("%d recieve %d , %d and %s\n", myRank, size, strcmp(_tuples, "all tuples have sent"), _tuples); 
			
			if(strcmp(_tuples, "all tuples have sent") == 0){
				break;
			}else{
				_six_tuples_fout << _tuples;
			}
			delete[] _tuples;
		}
		_six_tuples_fout.close();
		
		printf("%d recieve all all tuples!\n", myRank); 
		
		bool flag = _db.build("_distributed_gStore_tmp_rdf_triples.n3");
		if (flag)
		{
			cout << "import RDF file to database done." << endl;
		}
		else
		{
			cout << "import RDF file to database failed." << endl;
		}
		
		_db.setInternalVertices("_distributed_gStore_tmp_internal_vertices.txt");
		remove("_distributed_gStore_tmp_internal_vertices.txt");
		remove("_distributed_gStore_tmp_rdf_triples.n3");
		
		printf("%d finish loading data!\n", myRank); 
		delete[] _internal_vertices_arr;
	}
	
	MPI_Finalize();

	return 0;
}

