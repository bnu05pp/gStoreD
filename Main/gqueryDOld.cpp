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

#include "../Database/Database.h"
#include "../Util/Util.h"
#include <mpi.h>

using namespace std;

//WARN:cannot support soft links!

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
	//chdir(dirname(argv[0]));
#ifdef DEBUG
	Util util;
#endif

	if (argc == 1 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)
	{
		help();
		return 0;
	}
	//cout << "gquery..." << endl;
	if (argc < 2)
	{
		cerr << "error: lack of DB_store to be queried" << endl;
		return 0;
	}
	{
		//cout << "argc: " << argc << "\t";
		//cout << "DB_store:" << argv[1] << "\t";
		//cout << endl;
	}
	if (argc >= 3)
	{
		int myRank, p, size, i, j, k, l = 0;
		double partialResStart, partialResEnd;
		char* queryCharArr;
		char* partialResArr;
		int* crossingEdgeHashTable = new int[Util::MAX_CROSSING_EDGE_HASH_SIZE];
		int* crossingEdgeHashTagTable = new int[Util::MAX_CROSSING_EDGE_HASH_SIZE];
		memset(crossingEdgeHashTable, 0, Util::MAX_CROSSING_EDGE_HASH_SIZE);
		memset(crossingEdgeHashTagTable, 0, Util::MAX_CROSSING_EDGE_HASH_SIZE);
		MPI_Status status;
		MPI_Init(&argc, &argv);

		MPI_Comm_rank(MPI_COMM_WORLD,&myRank);
		MPI_Comm_size(MPI_COMM_WORLD,&p);
        if(myRank == 0) {
			double schedulingStart, schedulingEnd;
			
			string _query_str = Util::getQueryFromFile(argv[2]);
			queryCharArr = new char[1024];
			strcpy(queryCharArr, _query_str.c_str());
			size = strlen(queryCharArr);
			cout << "query : " << queryCharArr << endl;
			/*
			string query_file_str = string(argv[2]);
			query_file_str = query_file_str.substr(query_file_str.find("/") + 1);
			query_file_str = "log_" + query_file_str;
			ofstream log_output(query_file_str.c_str());
			string partial_res_str;
			*/

			for(i = 1; i < p; i++){
				MPI_Send(&size, 1, MPI_INT, i, 10, MPI_COMM_WORLD);
				MPI_Send(queryCharArr, size, MPI_CHAR, i, 10, MPI_COMM_WORLD);
			}
			partialResStart = MPI_Wtime();
			printf("The query has been sent!\n");
			
			ResultSet _result_set;
			int PPQueryVertexCount = -1, vec_size = 0, star_tag = 0;
			QueryTree::QueryForm query_form = QueryTree::Ask_Query;
			GeneralEvaluation parser_evaluation(NULL, NULL, NULL);
			
			parser_evaluation.onlyParseQuery(_query_str, PPQueryVertexCount, query_form, star_tag);
			
			if(query_form == QueryTree::Ask_Query){
			
				int fullTag = 0, partialResNum = 0, ask_query_res_tag = 0;
				unsigned long long sizeSum = 0;
				fullTag = (1 << PPQueryVertexCount) - 1;
				printf("PPQueryVertexCount = %d and fullTag = %d\n", PPQueryVertexCount, fullTag);
				vector<CrossingEdgeMappingVec> intermediate_results_vec(fullTag + 1);
				for(int i = 0; i < intermediate_results_vec.size(); i++){
					intermediate_results_vec[i].tag = i;
				}
				
				//ofstream log_output("log.txt");
				
				for(int pInt = 1; pInt < p; pInt++){
					MPI_Recv(&vec_size, 1, MPI_INT, pInt, 10, MPI_COMM_WORLD, &status);
					
					for(int vecIdx = 0; vecIdx < vec_size; vecIdx++){
					
						MPI_Recv(&size, 1, MPI_INT, pInt, 10, MPI_COMM_WORLD, &status);
						sizeSum += size;
						partialResArr = new char[size + 3];
						MPI_Recv(partialResArr, size, MPI_CHAR, pInt, 10, MPI_COMM_WORLD, &status);
						partialResArr[size] = 0;
						
						//log_output << "++++++++++++ " << pInt << " ++++++++++++" << endl;
						//log_output << partialResArr << endl;
						
						string textline(partialResArr);
						vector<string> resVec = Util::split(textline, "\n");
						partialResNum += resVec.size();
						//printf("processs %d : %s\n", pInt, resVec[0].c_str());
						for(i = 0; i < resVec.size(); i++){
							//curPartialResSize = resVec[i].length();
							vector<string> matchVec = Util::split(resVec[i], "\t");
							if((matchVec.size() % 4) != 1)
								continue;
								
							if(star_tag == 1){
								ask_query_res_tag = 1;
								break;
							}
							
							l = 0;
							for(k = 0; k < matchVec[matchVec.size() - 1].size(); k++)
							{
								l = l * 2 + matchVec[matchVec.size() - 1].at(k) - '0';
							}
							
							LEC curLEC;
							
							//printf("%s %d maps to tag %d\n", matchVec[matchVec.size() - 1].c_str(), pInt, l); 
							for(j = matchVec.size() - 2; j >= 0; j -= 4){
								CrossingEdgeMapping curCrossingEdgeMapping;
								curCrossingEdgeMapping.tail_query_id = atoi(matchVec[j - 2].c_str());
								curCrossingEdgeMapping.head_query_id = atoi(matchVec[j - 3].c_str());
								curCrossingEdgeMapping.mapping_str = matchVec[j] + "\t" + matchVec[j - 1];
								curCrossingEdgeMapping.fragmentID = pInt;
								curLEC.CrossingEdgeMappings.push_back(curCrossingEdgeMapping);
							}
							intermediate_results_vec[l].LECVec.push_back(curLEC);
						}

						delete[] partialResArr;
					}
				}

				partialResEnd = MPI_Wtime();

				// If there no exist partial match, the process terminate
				double time_cost_value = partialResEnd - partialResStart;
				printf("Communication cost %f s with %lld size!\n", time_cost_value, sizeSum);
				printf("There are %d LEC features.\n", partialResNum);
				//printf("There are %d inner matches.\n", finalPartialResSet.size());
				
				schedulingStart = MPI_Wtime();
				map< int, vector<int> > query_adjacent_list;

				for(int i = 0; i < intermediate_results_vec.size(); i++){
					if(intermediate_results_vec[i].LECVec.size() == 0){
						continue;
					}
					for(int j = i + 1; j < intermediate_results_vec.size(); j++){
						if(intermediate_results_vec[j].LECVec.size() == 0){
							continue;
						}
						if(Util::checkJoinable(intermediate_results_vec[i], intermediate_results_vec[j])){
							if(query_adjacent_list.count(i) == 0){
								vector<int> vec1;
								query_adjacent_list.insert(make_pair(i, vec1));
							}
							if(query_adjacent_list.count(j) == 0){
								vector<int> vec1;
								query_adjacent_list.insert(make_pair(j, vec1));
							}
							query_adjacent_list[i].push_back(j);
							query_adjacent_list[j].push_back(i);
						}
					}
				}
				
				for(int i = 0; i < intermediate_results_vec.size(); i++){
					if(query_adjacent_list.count(i) == 0){
						continue;
					}
					
					//printf("%d begin to search ! \n", i);
					queue< vector<int> > bfs_queue;
					queue<CrossingEdgeMappingVec> res_queue;
					vector<int> tmp_vec(1, i);
					bfs_queue.push(tmp_vec);
					res_queue.push(intermediate_results_vec[i]);
					while(bfs_queue.size()){
						vector<int> cur_bfs_state = bfs_queue.front();
						bfs_queue.pop();
						
						CrossingEdgeMappingVec tmpCrossingEdgeMappingVec = res_queue.front();
						res_queue.pop();
						
						int cur_mapping_vec_id = cur_bfs_state[cur_bfs_state.size() - 1];
						for(int j = 0; j < query_adjacent_list[cur_mapping_vec_id].size(); j++){
							if(query_adjacent_list[cur_mapping_vec_id][j] <= i || find(cur_bfs_state.begin(), cur_bfs_state.end(), query_adjacent_list[cur_mapping_vec_id][j]) != cur_bfs_state.end())
								continue;
							
							if(Util::checkJoinable(tmpCrossingEdgeMappingVec, intermediate_results_vec[query_adjacent_list[cur_mapping_vec_id][j]])){
								CrossingEdgeMappingVec newCrossingEdgeMappingVec;
								Util::HashLECFJoin(newCrossingEdgeMappingVec, tmpCrossingEdgeMappingVec, intermediate_results_vec[query_adjacent_list[cur_mapping_vec_id][j]]);
								
								if(newCrossingEdgeMappingVec.LECVec.size() != 0){
									vector<int> new_state(cur_bfs_state);
									new_state.push_back(query_adjacent_list[cur_mapping_vec_id][j]);
									bfs_queue.push(new_state);
									res_queue.push(newCrossingEdgeMappingVec);
									
									if(newCrossingEdgeMappingVec.tag == fullTag){
										break;
									}
								}
							}
						}
						
						if(res_queue.size() == 0 || res_queue.back().tag == fullTag){
							break;
						}
					}
					
					if(res_queue.size() != 0 && res_queue.back().tag == fullTag){
						ask_query_res_tag = 1; 
						break;
					}
				}
				
				schedulingEnd = MPI_Wtime();
				printf("Total cost %f s!\n", (schedulingEnd - partialResStart));
				
				if(ask_query_res_tag)
					printf("true.\n");
				else
					printf("false.\n");
			}else{
			
				map<string, int> URIIDMap;
				map<int, string> IDURIMap;
				int id_count = 0, cur_id = 0;
				int partialResNum = 0, finalResNum = 0, aResNum = 0, vec_size = 0;
				unsigned long long sizeSum = 0;
				set< vector<int> > finalPartialResSet;
				int fullTag = (1 << PPQueryVertexCount) - 1;
				
				vector< PPPartialResVec > partialResVec(fullTag + 1);
				for(i = 0; i < partialResVec.size(); i++){
					partialResVec[i].match_pos = i;
				}
				ofstream log_output("log.txt");

				for(int pInt = 1; pInt < p; pInt++){
					MPI_Recv(&vec_size, 1, MPI_INT, pInt, 10, MPI_COMM_WORLD, &status);
					aResNum = 0;
					for(int vecIdx = 0; vecIdx < vec_size; vecIdx++){
						MPI_Recv(&size, 1, MPI_INT, pInt, 10, MPI_COMM_WORLD, &status);
						sizeSum += size;
						partialResArr = new char[size + 3];
						MPI_Recv(partialResArr, size, MPI_CHAR, pInt, 10, MPI_COMM_WORLD, &status);
						partialResArr[size] = 0;
						
						//printf("++++++++++++ %d ++++++++++++\n%s\n", pInt, partialResArr);
						//log_output << "++++++++++++ " << pInt << " ++++++++++++" << endl;
						//log_output << partialResArr << endl;
						//partial_res_str = string(partialResArr);
						
						string textline(partialResArr);
						vector<string> resVec = Util::split(textline, "\n");
						for(i = 0; i < resVec.size(); i++){
							//curPartialResSize = resVec[i].length();
							vector<string> matchVec = Util::split(resVec[i], "\t");
							if(matchVec.size() != PPQueryVertexCount)
								continue;
							PPPartialRes newPPPartialRes;
							
							if(star_tag == 1){
								for(j = 0; j < matchVec.size(); j++){
									newPPPartialRes.TagVec.push_back('1');
									if(URIIDMap.count(matchVec[j]) == 0){
										URIIDMap.insert(make_pair(matchVec[j], id_count));
										IDURIMap.insert(make_pair(id_count, matchVec[j]));
										id_count++;
									}
									cur_id = URIIDMap[matchVec[j]];
									newPPPartialRes.MatchVec.push_back(cur_id);
								}
								finalPartialResSet.insert(newPPPartialRes.MatchVec);
								continue;
							}
							
							l = 0;
							for(j = 0; j < matchVec.size(); j++){
								if(strcmp(matchVec[j].c_str(),"-1") != 0){
									l = l * 2 + matchVec[j].at(0) - '0';
									
									newPPPartialRes.TagVec.push_back(matchVec[j].at(0));
									matchVec[j].erase(0, 1);

									if(URIIDMap.count(matchVec[j]) == 0){
										URIIDMap.insert(make_pair(matchVec[j], id_count));
										IDURIMap.insert(make_pair(id_count, matchVec[j]));
										id_count++;
									}
									cur_id = URIIDMap[matchVec[j]];
								}else{
									l = l * 2;
									newPPPartialRes.TagVec.push_back('2');
									cur_id = -1;
								}
								newPPPartialRes.MatchVec.push_back(cur_id);
								
								//if('1' == newPPPartialRes.TagVec[newPPPartialRes.TagVec.size() - 1])
								//	match_pos_vec.push_back(j);
							}
							newPPPartialRes.FragmentID = pInt;
							newPPPartialRes.ID = partialResNum;

							if(0 == Util::isFinalResult(newPPPartialRes)){
								
								partialResVec[l].PartialResList.push_back(newPPPartialRes);
								aResNum++;
								partialResNum++;
							}else{
								finalPartialResSet.insert(newPPPartialRes.MatchVec);
							}
							
							//printf("%d containing %d has the result: %s\n", pInt, l, resVec[i].c_str());
						}
						delete[] partialResArr;
					}
					printf("There are %d partial results and %d final results in Client %d!\n", aResNum, finalPartialResSet.size() - finalResNum, pInt);
					finalResNum = finalPartialResSet.size();
				}

				partialResEnd = MPI_Wtime();

				// If there no exist partial match, the process terminate
				double time_cost_value = partialResEnd - partialResStart;
				printf("Communication cost %f s!\n", time_cost_value);
				printf("There are %d partial results with %lld size.\n", partialResNum, sizeSum);
				printf("There are %d inner matches.\n", finalPartialResSet.size());
				
				schedulingStart = MPI_Wtime();
				
				map< int, vector<int> > partial_res_adjacent_list;
				//stringstream adj_list_ss;

				for(int i = 0; i < partialResVec.size(); i++){
					if(partialResVec[i].PartialResList.size() == 0){
						continue;
					}
					//adj_list_ss << i << " : ";
					for(int j = i + 1; j < partialResVec.size(); j++){
						if(partialResVec[j].PartialResList.size() == 0){
							continue;
						}
						if(Util::checkJoinable(partialResVec[i], partialResVec[j], i, j) != -1){
							if(partial_res_adjacent_list.count(i) == 0){
								vector<int> vec1;
								partial_res_adjacent_list.insert(make_pair(i, vec1));
							}
							if(partial_res_adjacent_list.count(j) == 0){
								vector<int> vec1;
								partial_res_adjacent_list.insert(make_pair(j, vec1));
							}
							partial_res_adjacent_list[i].push_back(j);
							partial_res_adjacent_list[j].push_back(i);
							//adj_list_ss << j << "\t";
						}
					}
					
					//adj_list_ss << endl;
				}
				
				vector< vector<int> > join_order_vec = Util::findMultipleJoinOrder(partial_res_adjacent_list, partialResVec, fullTag);
				
				for(i = 0; i < join_order_vec.size(); i++){
					
					PPPartialResVec tmpPPPartialResVec;
					tmpPPPartialResVec.PartialResList.assign(partialResVec[join_order_vec[i][0]].PartialResList.begin(), partialResVec[join_order_vec[i][0]].PartialResList.end());
					tmpPPPartialResVec.match_pos = partialResVec[join_order_vec[i][0]].match_pos;
					
					for(j = 1; j < join_order_vec[i].size(); j++){
						
						int cur_match_pos = Util::checkJoinable(tmpPPPartialResVec, partialResVec[join_order_vec[i][j]], tmpPPPartialResVec.match_pos, partialResVec[join_order_vec[i][j]].match_pos);
						tmpPPPartialResVec.match_pos = tmpPPPartialResVec.match_pos | partialResVec[join_order_vec[i][j]].match_pos;
						
						map<int, vector<PPPartialRes> > tmpPartialResMap;
						for(k = 0; k < partialResVec[join_order_vec[i][j]].PartialResList.size(); k++){
							
							if(tmpPartialResMap.count(partialResVec[join_order_vec[i][j]].PartialResList[k].MatchVec[cur_match_pos]) == 0){
								vector<PPPartialRes> tmpVec;
								tmpPartialResMap.insert(make_pair(partialResVec[join_order_vec[i][j]].PartialResList[k].MatchVec[cur_match_pos], tmpVec));
							}
							tmpPartialResMap[partialResVec[join_order_vec[i][j]].PartialResList[k].MatchVec[cur_match_pos]].push_back(partialResVec[join_order_vec[i][j]].PartialResList[k]);
						}
						Util::HashJoin(finalPartialResSet, tmpPPPartialResVec.PartialResList, tmpPartialResMap, p, cur_match_pos);
						
						if(tmpPPPartialResVec.PartialResList.size() == 0){
							break;
						}
					}
				}
				
				printf("There are %d final matches.\n", finalPartialResSet.size());
				schedulingEnd = MPI_Wtime();
				time_cost_value = schedulingEnd - partialResStart;
				printf("Total cost %f s!\n", time_cost_value);
				
				//log_output << partial_res_str << endl;
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
			}
			
		}else{
			string db_folder = string(argv[1]);
			Database _db(db_folder);
			_db.load();
			printf("Client %d finish loading!\n", myRank);
			
			//ofstream log_output_partial("log_partial.txt");
	
			MPI_Recv(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, &status);
			queryCharArr = new char[size];
			MPI_Recv(queryCharArr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
			queryCharArr[size - 1] = 0;
			
			//stringstream log_ss;
			
			partialResStart = MPI_Wtime();
			string _query_str(queryCharArr);
			
			ResultSet _rs;
			vector<string> all_lpm_str_vec;
			vector<string> lpm_str_vec;
			vector< vector<int> > res_crossing_edges_vec;
			vector<int> all_crossing_edges_vec;
			_db.queryCrossingEdge(_query_str, _rs, lpm_str_vec, res_crossing_edges_vec, all_crossing_edges_vec, myRank, stdout);
			
			stringstream all_lpm_ss;
			for(int i = 0; i < lpm_str_vec.size(); i++){
				all_lpm_ss << lpm_str_vec[i] << endl;
			}
			all_lpm_str_vec.push_back(all_lpm_ss.str());
			//printf("After filtering, Client %d remains %d results\n", myRank, commResNum);
			
			partialResEnd = MPI_Wtime();
			
			//============================= communication of partial results =============================
			
			double time_cost_value = partialResEnd - partialResStart;
			printf("Finding local partial matches costs %f s in Client %d with vec_size = %d\n", time_cost_value, myRank, all_lpm_str_vec.size());
			
			size = all_lpm_str_vec.size();
			MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
			for(int i = 0; i < all_lpm_str_vec.size(); i++){
				partialResArr = new char[all_lpm_str_vec[i].size() + 3];
				strcpy(partialResArr, all_lpm_str_vec[i].c_str());
				size = strlen(partialResArr);
				
				MPI_Send(&size, 1, MPI_INT, 0, 10, MPI_COMM_WORLD);
				MPI_Send(partialResArr, size, MPI_CHAR, 0, 10, MPI_COMM_WORLD);
				
				//log_ss << " with size " << size;
				
				delete[] partialResArr;
			}
		}
		
		delete[] queryCharArr;
		delete[] crossingEdgeHashTable;
		delete[] crossingEdgeHashTagTable;

		MPI_Finalize();
	}
	return 0;
}