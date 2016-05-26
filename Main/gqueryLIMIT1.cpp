/*=============================================================================
# Filename: gquery.cpp
# Author: Bookug Lobert
# Mail: 1181955272@qq.com
# Last Modified: 2015-10-20 12:23
# Description: query a database, there are several ways to use this program:
1. ./gquery                                        print the help message
2. ./gquery --help                                 simplified as -h, equal to 1
3. ./gquery db_folder query_path                   load query from given path fro given database
4. ./gquery db_folder                              load the given database and open console
=============================================================================*/

//#include <iostream>
#include <stdio.h>
//#include <stdlib.h>
//#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>
#include "../Database/Database.h"
#include "../Util/Util.h"
#include <mpi.h>

using namespace std;

struct Config{
	string db_folder;
};

struct PPPartialRes{
	vector<int> MatchVec;
	vector<char> TagVec;
	int FragmentID;
	int ID;
};

struct PPPartialResVec{
	vector<PPPartialRes> PartialResList;
	int match_pos;
};

//WARN:cannot support soft links!
Config
loadConfig(const char* _file_path)
{
#ifdef DEBUG_PRECISE
    printf("file to open: %s\n", _file_path);
#endif
    string s;
	Config _config;
	ifstream fin(_file_path);	
    if(!fin)
    {
        printf("can not open: %s\n", _file_path);
		exit(1);
    }else{		
		getline(fin,s);
		_config.db_folder = s;
	}

    return _config;
}

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

void QuickSort(vector<PPPartialRes>& res1, int matchPos, int low, int high){
	if(low >= high){
		return;
	}
	int first = low;
	int last = high;
	/*用字表的第一个记录作为枢轴*/
	PPPartialRes key;
	key.MatchVec.assign(res1[first].MatchVec.begin(), res1[first].MatchVec.end());
	key.TagVec.assign(res1[first].TagVec.begin(), res1[first].TagVec.end());
	key.FragmentID = res1[first].FragmentID;

	while(first < last){
		while(first < last && res1[last].MatchVec[matchPos] >= key.MatchVec[matchPos]){
			--last;
		}
		/*将比第一个小的移到低端*/
		res1[first].MatchVec.assign(res1[last].MatchVec.begin(), res1[last].MatchVec.end());
		res1[first].TagVec.assign(res1[last].TagVec.begin(), res1[last].TagVec.end());
		res1[first].FragmentID = res1[last].FragmentID;
		while(first < last && res1[first].MatchVec[matchPos] <= key.MatchVec[matchPos]){
			++first;
		}

		res1[last].MatchVec.assign(res1[first].MatchVec.begin(), res1[first].MatchVec.end());
		res1[last].TagVec.assign(res1[first].TagVec.begin(), res1[first].TagVec.end());
		res1[last].FragmentID = res1[first].FragmentID;
		/*将比第一个大的移到高端*/
	}
	/*枢轴记录到位*/
	res1[first].MatchVec.assign(key.MatchVec.begin(), key.MatchVec.end());
	res1[first].TagVec.assign(key.TagVec.begin(), key.TagVec.end());
	res1[first].FragmentID = key.FragmentID;
	
	QuickSort(res1, matchPos, low, first-1);
	QuickSort(res1, matchPos, first+1, high);
}

int isFinalResult(PPPartialRes curPPPartialRes){
	for(int i = 0; i < curPPPartialRes.TagVec.size(); i++){
		if('1' != curPPPartialRes.TagVec[i])
			return 0;
	}
	return 1;
}

bool myfunction0(PPPartialResVec v1, PPPartialResVec v2){
	if(v1.PartialResList.size() != v2.PartialResList.size())
		return (v1.PartialResList.size() < v2.PartialResList.size());
	return (v1.match_pos < v2.match_pos);
}

void MergeJoin(set< vector<int> >& finalPartialResSet, vector<PPPartialRes>& res1, vector<PPPartialRes>& res2, int fragmentNum, int matchPos){
	if(0 == res1.size()){
		return;
	}
	
	QuickSort(res1, matchPos, 0, res1.size() - 1);
	QuickSort(res2, matchPos, 0, res2.size() - 1);
	//cout << "res1.size() = " << res1.size() << ", res2.size() = " << res2.size() << endl;

	vector<PPPartialRes> new_res;
	int first_pos = 0, second_pos = 0, tag = 0;
	int l_iter = 0, r_iter = 0, len = res1[0].MatchVec.size();
	while (l_iter < res1.size() && r_iter < res2.size()) {
	
		if (l_iter == res1.size()) {
			r_iter++;
		} else if('1' == res1[l_iter].TagVec[matchPos]){
			new_res.push_back(res1[l_iter]);
			l_iter++;
			continue;
		} else if (r_iter == res2.size()) {
			l_iter++;
		} else if (res1[l_iter].MatchVec[matchPos] < res2[r_iter].MatchVec[matchPos]) {
			l_iter++;
		} else if (res1[l_iter].MatchVec[matchPos] > res2[r_iter].MatchVec[matchPos]) {
			r_iter++;
		} else {
			int l_iter_end = l_iter + 1, r_iter_end = r_iter + 1;

			while (l_iter_end < res1.size()
					&& res1[l_iter_end].MatchVec[matchPos] == res1[l_iter].MatchVec[matchPos]) {
				l_iter_end++;
				if (l_iter_end == res1.size()) {
					break;
				}
			}
			while (r_iter_end < res2.size()
					&& res2[r_iter_end].MatchVec[matchPos] == res2[r_iter].MatchVec[matchPos]) {
				r_iter_end++;
				if (r_iter_end == res2.size()) {
					break;
				}
			}

			for (int i = l_iter; i < l_iter_end; i++) {
				for (int j = r_iter; j < r_iter_end; j++) {
					if(res1[i].FragmentID == res2[j].FragmentID)
						continue;
						
					tag = 0;
					PPPartialRes curPPPartialRes;
					curPPPartialRes.TagVec.assign(len, '0');
					curPPPartialRes.FragmentID = fragmentNum + res1[i].FragmentID;
					for(int k = 0; k < len; k++){
						if(res1[i].MatchVec[k] != -1 && res2[j].MatchVec[k] != -1
							&& res1[i].MatchVec[k] != res2[j].MatchVec[k]){

							tag = 1;
							break;
						}else if(res1[i].MatchVec[k] == -1 && res2[j].MatchVec[k] != -1){
							curPPPartialRes.TagVec[k] = res2[j].TagVec[k];
							curPPPartialRes.MatchVec.push_back(res2[j].MatchVec[k]);
						}else if(res1[i].MatchVec[k] != -1 && res2[j].MatchVec[k] == -1){
							curPPPartialRes.TagVec[k] = res1[i].TagVec[k];
							curPPPartialRes.MatchVec.push_back(res1[i].MatchVec[k]);
						}else{
							if('1' == res1[i].TagVec[k] || '1' == res2[j].TagVec[k])
								curPPPartialRes.TagVec[k] = '1';
							curPPPartialRes.MatchVec.push_back(res1[i].MatchVec[k]);
						}
					}
					
					//cout << "tag = " << tag << endl;
					if(tag == 1)
						continue;

					if(0 == isFinalResult(curPPPartialRes)){
						new_res.push_back(curPPPartialRes);
					}else{
						finalPartialResSet.insert(curPPPartialRes.MatchVec);
					}
					
				}
			}

			r_iter = r_iter_end;
			l_iter = l_iter_end;
		}
	}

	//res1.cur_res_list.clear();
	res1.assign(new_res.begin(), new_res.end());
	//res1.tag_vec.assign(new_tag_vec.begin(), new_tag_vec.end());
}

//WARN:cannot support soft links!
string
getQueryFromFile(const char* _file_path)
{
#ifdef DEBUG_PRECISE
    printf("file to open: %s\n", _file_path);
#endif
    char buf[10000];
    std::string query_file;

    ifstream fin(_file_path);
    if(!fin)
    {
        printf("can not open: %s\n", _file_path);
        return "";
    }

    memset(buf, 0, sizeof(buf));
    stringstream _ss;
    while(!fin.eof())
    {
        fin.getline(buf, 9999);
        _ss << buf << "\n";
    }
    fin.close();

    return _ss.str();
}

void
help()
{
    printf("\
			/*=============================================================================\n\
# Filename: gquery.cpp\n\
# Author: Bookug Lobert\n\
# Mail: 1181955272@qq.com\n\
# Last Modified: 2015-10-20 12:23\n\
# Description: query a database, there are several ways to use this program:\n\
1. ./gquery                                        print the help message\n\
2. ./gquery --help                                 simplified as -h, equal to 1\n\
3. ./gquery db_folder query_path                   load query from given path fro given database\n\
4. ./gquery db_folder                              load the given database and open console\n\
=============================================================================*/\n");
}

int
main(int argc, char * argv[])
{
#ifdef DEBUG
    Util util;
#endif
/*
    if(argc == 1 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)
    {
        help();
        return 0;
    }

    cout << "gquery..." << endl;
*/
    if(argc < 2)
    {
        cerr << "error: lack of DB_store to be queried" << endl;
        return 0;
    }
	
	int myRank, p, size, i, j, k, l = 0;
	double partialResStart, partialResEnd;
	char* queryCharArr;
	char* partialResArr;
	MPI_Status status;
	MPI_Init(&argc, &argv);

	MPI_Comm_rank(MPI_COMM_WORLD,&myRank);
	MPI_Comm_size(MPI_COMM_WORLD,&p);
	
	if(myRank == 0) {
		double schedulingStart, schedulingEnd;
		
		string _query_str = getQueryFromFile(argv[2]);
		queryCharArr = new char[1024];
		strcpy(queryCharArr, _query_str.c_str());
		size = strlen(queryCharArr);
		cout << "query : " << queryCharArr << endl;

		for(i = 1; i < p; i++){
			MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
			MPI_Send(queryCharArr, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
		}
		printf("Query has been sent!\n");
		partialResStart = MPI_Wtime();
		
		map<string, int> URIIDMap;
		map<int, string> IDURIMap;
		int id_count = 0, cur_id = 0;
		int partialResNum = 0, sizeSum = 0, PPQueryVertexCount = 0;
		set< vector<int> > finalPartialResSet;
		
		for(int pInt = 1; pInt < p; pInt++){
			MPI_Recv(&PPQueryVertexCount, 1, MPI_INT, pInt, 10, MPI_COMM_WORLD, &status);
		}
		vector< PPPartialResVec > partialResVec(PPQueryVertexCount);
		for(i = 0; i < partialResVec.size(); i++){
			partialResVec[i].match_pos = i;
		}

		for(int pInt = 1; pInt < p; pInt++){
			MPI_Recv(&size, 1, MPI_INT, pInt, 10, MPI_COMM_WORLD, &status);
			sizeSum += size;
			partialResArr = new char[size + 3];
			MPI_Recv(partialResArr, size, MPI_CHAR, pInt, 10, MPI_COMM_WORLD, &status);
			partialResArr[size] = 0;
			
			//printf("++++++++++++ %d ++++++++++++\n%s\n", pInt, partialResArr);
			
			string textline(partialResArr);
			vector<string> resVec = split(textline, "\n");
			for(i = 0; i < resVec.size(); i++){
				//curPartialResSize = resVec[i].length();
				vector<string> matchVec = split(resVec[i], "\t");
				if(matchVec.size() != PPQueryVertexCount)
					continue;
				PPPartialRes newPPPartialRes;
				vector<int> match_pos_vec;
				for(j = 0; j < matchVec.size(); j++){
					if(strcmp(matchVec[j].c_str(),"-1") != 0){
						newPPPartialRes.TagVec.push_back(matchVec[j].at(0));
						matchVec[j].erase(0, 1);

						if(URIIDMap.count(matchVec[j]) == 0){
							URIIDMap.insert(make_pair(matchVec[j], id_count));
							IDURIMap.insert(make_pair(id_count, matchVec[j]));
							id_count++;
						}
						cur_id = URIIDMap[matchVec[j]];
					}else{
						newPPPartialRes.TagVec.push_back('2');
						cur_id = -1;
					}
					newPPPartialRes.MatchVec.push_back(cur_id);
					
					if('1' == newPPPartialRes.TagVec[newPPPartialRes.TagVec.size() - 1])
						match_pos_vec.push_back(j);
				}
				newPPPartialRes.FragmentID = pInt;
				newPPPartialRes.ID = partialResNum;

				if(0 == isFinalResult(newPPPartialRes)){
					for(j = 0; j < match_pos_vec.size(); j++){
						partialResVec[match_pos_vec[j]].PartialResList.push_back(newPPPartialRes);
					}
					partialResNum++;
				}else{
					finalPartialResSet.insert(newPPPartialRes.MatchVec);
				}
			}

			delete[] partialResArr;
		}

		partialResEnd = MPI_Wtime();

		// If there no exist partial match, the process terminate
		double time_cost_value = partialResEnd - partialResStart;
		printf("Communication cost %f s!\n", time_cost_value);
		printf("There are %d partial results with %d size.\n", partialResNum);
		printf("There are %d inner matches.\n", finalPartialResSet.size());
		
		schedulingStart = MPI_Wtime();

		sort(partialResVec.begin(), partialResVec.end(), myfunction0);
		
		vector<int> match_pos_vec;
		int tag = 0;
		match_pos_vec.push_back(partialResVec[0].match_pos);
		if(0 != partialResVec.size()){
		
			stringstream intermediate_strm;

			for(i = 1; i < partialResVec.size(); i++){
				//cout << "###########  " << partialResVec[i].match_pos << endl;
				
				vector<PPPartialRes> tmpPartialResVec;
				for(j = 0; j < partialResVec[i].PartialResList.size(); j++){
					tag = 0;
					for(k = 0; k < match_pos_vec.size(); k++){
						if('1' == partialResVec[i].PartialResList[j].TagVec[match_pos_vec[k]]){
							tag = 1;
							break;
						}
					}
					if(0 == tag)
						tmpPartialResVec.push_back(partialResVec[i].PartialResList[j]);
				}
				if(tmpPartialResVec.size() == 0){
					match_pos_vec.push_back(partialResVec[i].match_pos);
					continue;
				}
				/*
				for(j = 0; j < partialResVec[0].PartialResList.size(); j++){
					vector<int> tempVec = partialResVec[0].PartialResList[j].MatchVec;
					for(l = 0; l < tempVec.size(); l++){
						if(-1 != tempVec[l]){
							intermediate_strm << IDURIMap[tempVec[l]] << "\t";
						}else{
							intermediate_strm << -1 << "\t";
						}
					}
					intermediate_strm << endl;
				}
				printf("~~~~~~~~~~~~~ 1 ~~~~~~~~~~~~~\n%s\n", intermediate_strm.str().c_str());
				intermediate_strm.str("");
			
				for(j = 0; j < tmpPartialResVec.size(); j++){
					vector<int> tempVec = tmpPartialResVec[j].MatchVec;
					for(l = 0; l < tempVec.size(); l++){
						if(-1 != tempVec[l]){
							intermediate_strm << IDURIMap[tempVec[l]] << "\t";
						}else{
							intermediate_strm << -1 << "\t";
						}
					}
					intermediate_strm << endl;
				}
				printf("~~~~~~~~~~~~~ %d ~~~~~~~~~~~~~\n%s\n", partialResVec[i].match_pos, intermediate_strm.str().c_str());
				intermediate_strm.str("");
				
				cout << i << ": " << partialResVec[i].match_pos << " " << partialResVec[i].PartialResList.size() << " " << tmpPartialResVec.size() << " " << partialResVec[0].PartialResList.size() << endl;
				*/
				MergeJoin(finalPartialResSet, partialResVec[0].PartialResList, tmpPartialResVec, p, partialResVec[i].match_pos);
				match_pos_vec.push_back(partialResVec[i].match_pos);
				/*
				for(j = 0; j < partialResVec[0].PartialResList.size(); j++){
					vector<int> tempVec = partialResVec[0].PartialResList[j].MatchVec;
					for(l = 0; l < tempVec.size(); l++){
						if(-1 != tempVec[l]){
							intermediate_strm << IDURIMap[tempVec[l]] << "\t";
						}else{
							intermediate_strm << -1 << "\t";
						}
					}
					intermediate_strm << endl;
				}
				printf("============result============\n%s\n", intermediate_strm.str().c_str());
				intermediate_strm.str("");
*/
				if(partialResVec[0].PartialResList.size() == 0){
					break;
				}
			}
		}
		
		printf("There are %d final matches.\n", finalPartialResSet.size());
		//char* resFile = new char[128];
		//sprintf(resFile, "finalRes.txt");
		ofstream res_output("finalRes.txt");
		set< vector<int> >::iterator iter = finalPartialResSet.begin();
		while(iter != finalPartialResSet.end()){
			vector<int> tempVec = *iter;
			for(l = 0; l < tempVec.size(); l++){
				res_output << IDURIMap[tempVec[l]] << "\t";
			}
			res_output << endl;
			//total_res_count++;
			iter++;
		}
		res_output.close();
		
	}else{
		//ofstream log_output("log.txt");
		//Config _config = loadConfig("config.ini");
		//log_output << "begin loading DB_store" << endl;
		string db_folder = string(argv[1]);
		Database _db(db_folder);
		_db.load();
		//cout << db_folder.c_str() << 
		//cout << "finish loading DB_store" << db_folder << endl;
		
		MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
		queryCharArr = new char[size];
		MPI_Recv(queryCharArr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
		queryCharArr[size - 1] = 0;
		string _query_str(queryCharArr);
		
		partialResStart = MPI_Wtime();
		ResultSet _rs;
		string partialResStr;
		
		SPARQLquery cur_sparql_query = _db.query(_query_str, _rs, partialResStr, myRank);
		BasicQuery* basic_query=&(cur_sparql_query.getBasicQuery(0));
		_db.join_pe(basic_query, partialResStr);
		int cur_var_num = basic_query->getVarNum();
		
		partialResArr = new char[partialResStr.size() + 3];
		strcpy(partialResArr, partialResStr.c_str());
		size = strlen(partialResArr);
		
		MPI_Send(&cur_var_num, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
		MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);		
//		cout << myRank << " : " << partialResArr << endl;
		MPI_Send(partialResArr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD);
		
		delete[] partialResArr;
	}
	
	delete[] queryCharArr;
	
	MPI_Finalize();
/*
    // read query from file.
    if (argc >= 3)
    {
		string query = getQueryFromFile(argv[2]);
        if (query.empty())
        {
            return 0;
        }
        printf("query is:\n%s\n\n", query.c_str());
        ResultSet _rs;
        _db.query(query, _rs, stdout);
		
        if (argc >= 4)
        {
            Util::save_to_file(argv[3], _rs.to_str());
        }
		
        return 0;
    }

    // read query file path from terminal.
    // BETTER: sighandler ctrl+C/D/Z
    string query;
    //char resolved_path[PATH_MAX+1];
#ifdef READLINE_ON
    char *buf, prompt[] = "gsql>";
    //const int commands_num = 3;
    //char commands[][20] = {"help", "quit", "sparql"};
    printf("Type `help` for information of all commands\n");
	printf("Type `help command_t` for detail of command_t\n");
    rl_bind_key('\t', rl_complete);
    while(true)
    {
        buf = readline(prompt);
        if(buf == NULL)
            continue;
        else
            add_history(buf);
        if(strncmp(buf, "help", 4) == 0)
        {
			if(strcmp(buf, "help") == 0)
			{
            //print commands message
            printf("help - print commands message\n");
            printf("quit - quit the console normally\n");
            printf("sparql - load query from the second argument\n");
			}
			else
			{
				//TODO: sparql path > file	
			}
            continue;
        }
        else if(strcmp(buf, "quit") == 0)
            break;
        else if(strncmp(buf, "sparql", 6) != 0)
        {
            printf("unknown commands\n");
            continue;
        }
        std::string query_file;
        //BETTER:build a parser for this console
		bool ifredirect = false;
		char* rp = buf;
		while(*rp != '\0')
		{
			if(*rp == '>')
			{
				ifredirect = true;
				break;
			}
			rp++;
		}
        char* p = buf + strlen(buf) - 1;
		FILE* fp = stdout;      ///default to output on screen
		if(ifredirect)
		{
			char* tp = p;
			while(*tp == ' ' || *tp == '\t')
				tp--;
			*(tp+1) = '\0';
			tp = rp + 2;
			while(*tp == ' ' || *tp == '\t')
				tp++;
			fp = fopen(tp, "w");	//NOTICE:not judge here!
			p = rp - 2;					//NOTICE: all separated with ' ' or '\t'
		}
        while(*p == ' ' || *p == '\t')	//set the end of path
            p--;
        *(p+1) = '\0';
        p = buf + 6;
        while(*p == ' ' || *p == '\t')	//acquire the start of path
            p++;
        //TODO: support the soft links(or hard links)
        //there are also readlink and getcwd functions for help
        //http://linux.die.net/man/2/readlink
        //NOTICE:getcwd and realpath cannot acquire the real path of file
        //in the same directory and the program is executing when the
        //system starts running
        //NOTICE: use realpath(p, NULL) is ok, but need to free the memory
        char* q = realpath(p, NULL);	//QUERY:still not work for soft links
#ifdef DEBUG_PRECISE
        printf("%s\n", p);
#endif
        if(q == NULL)
        {
			printf("invalid path!\n");
			free(q);
			free(buf);
			continue;
        }
        else
			printf("%s\n", q);
        //query = getQueryFromFile(p);
        query = getQueryFromFile(q);
        if(query.empty())
        {
			free(q);
            //free(resolved_path);
            free(buf);
			if(ifredirect)
				fclose(fp);
            continue;
        }
        cout << "query is:" << endl;
        cout << query << endl << endl;
        ResultSet _rs;
        _db.query(query, _rs, fp);
        //test...
        //std::string answer_file = query_file+".out";
        //Util::save_to_file(answer_file.c_str(), _rs.to_str());
		free(q);
        //free(resolved_path);
        free(buf);
		if(ifredirect)
			fclose(fp);
#ifdef DEBUG_PRECISE
        //printf("after buf freed!\n");
#endif
    }
#else					//DEBUG:this not work!
    while(true)
    {
        cout << "please input query file path:" << endl;
        std::string query_file;
        cin >> query_file;
        //char* q = realpath(query_file.c_str(), NULL);
        string query = getQueryFromFile(query_file.c_str());
        if(query.empty())
        {
            //free(resolved_path);
            continue;
        }
        cout << "query is:" << endl;
        cout << query << endl << endl;
        ResultSet _rs;
        _db.query(query, _rs, stdout);
        //free(resolved_path);
    }
#endif
*/
    return 0;
}

