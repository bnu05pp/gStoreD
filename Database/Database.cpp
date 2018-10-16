/*=============================================================================
# Filename:		Database.cpp
# Author: Bookug Lobert
# Mail: 1181955272@qq.com
# Last Modified:	2016-09-11 15:27
# Description: originally written by liyouhuan, modified by zengli and chenjiaqi
=============================================================================*/

#include "Database.h"

using namespace std;

Database::Database()
{
	this->name = "";
	string store_path = ".";
	this->signature_binary_file = "signature.binary";
	this->six_tuples_file = "six_tuples";
	this->db_info_file = "db_info_file.dat";

	string kv_store_path = store_path + "/kv_store";
	this->kvstore = new KVstore(kv_store_path);

	string vstree_store_path = store_path + "/vs_store";
	this->vstree = new VSTree(vstree_store_path);

    string stringindex_store_path = store_path + "/stringindex_store";
    this->stringindex = new StringIndex(stringindex_store_path);

	this->encode_mode = Database::STRING_MODE;
	this->is_active = false;
	this->sub_num = 0;
	this->pre_num = 0;
	this->literal_num = 0;
	this->entity_num = 0;
	this->triples_num = 0;

	this->join = NULL;

	//this->resetIDinfo();
	this->initIDinfo();
}

Database::Database(string _name)
{
	this->name = _name;
	string store_path = this->name;

	this->signature_binary_file = "signature.binary";
	this->six_tuples_file = "six_tuples";
	this->db_info_file = "db_info_file.dat";

	string kv_store_path = store_path + "/kv_store";
	this->kvstore = new KVstore(kv_store_path);

	string vstree_store_path = store_path + "/vs_store";
	this->vstree = new VSTree(vstree_store_path);

    string stringindex_store_path = store_path + "/stringindex_store";
    this->stringindex = new StringIndex(stringindex_store_path);

	this->encode_mode = Database::STRING_MODE;
	this->is_active = false;
	this->sub_num = 0;
	this->pre_num = 0;
	this->literal_num = 0;
	this->entity_num = 0;
	this->triples_num = 0;

	this->join = NULL;

	//this->resetIDinfo();
	this->initIDinfo();
}



//==================================================================================================================================================
//NOTICE:
//there are 3 ways to manage a dynamic-garbage ID list
//1. push all unused IDs into list, get top each time to alloc, push freed one to tail, push more large one if NULL
//(can use bit array if stored in disk)
//2. when free, change the mapping for non-free one whose ID is the largest currently
//(sometimes maybe the copy cost is very high)
//3. NULL first and push one if freed, get one if not empty(limit+1 if NULL)
//(when stored in disk, maybe consume larger space)

void
Database::initIDinfo()
{
	this->free_id_file_entity = this->getStorePath() + "/freeEntityID.dat";
	this->limitID_entity = -1;
	this->freelist_entity = NULL;

	this->free_id_file_literal = this->getStorePath() + "/freeLiteralID.dat";
	this->limitID_literal = -1;
	this->freelist_literal = NULL;

	this->free_id_file_predicate = this->getStorePath() + "/freePredicateID.dat";
	this->limitID_predicate = -1;
	this->freelist_predicate = NULL;
}

void
Database::resetIDinfo()
{
	this->initIDinfo();

	this->limitID_entity = Database::START_ID_NUM;
	//NOTICE:add base LITERAL_FIRST_ID for literals
	this->limitID_literal = Database::START_ID_NUM;
	this->limitID_predicate = Database::START_ID_NUM;

	BlockInfo* tmp = NULL;
	for (int i = Database::START_ID_NUM - 1; i >= 0; --i)
	{
		tmp = new BlockInfo(i, this->freelist_entity);
		this->freelist_entity = tmp;
		tmp = new BlockInfo(i, this->freelist_literal);
		this->freelist_literal = tmp;
		tmp = new BlockInfo(i, this->freelist_predicate);
		this->freelist_predicate = tmp;
	}
}

void
Database::readIDinfo()
{
	this->initIDinfo();

	FILE* fp = NULL;
	int t = -1;
	BlockInfo* bp = NULL;

	//printf("this->free_id_file_entity.c_str() = %s\n", this->free_id_file_entity.c_str());
	fp = fopen(this->free_id_file_entity.c_str(), "r");
	if (fp == NULL)
	{
		cerr << "read entity id info error" << endl;
		return;
	}
	//QUERY:this will reverse the original order, if change?
	//Notice that if we cannot ensure that IDs are uporder and continuous, we can
	//not keep an array for IDs like _entity_bitset
	BlockInfo *tmp = NULL, *cur = NULL;
	fread(&(this->limitID_entity), sizeof(int), 1, fp);
	fread(&t, sizeof(int), 1, fp);
	while (!feof(fp))
	{
		//if(t == 14912)
		//{
			//cout<<"Database::readIDinfo() - get 14912"<<endl;
		//}
		//bp = new BlockInfo(t, this->freelist_entity);
		//this->freelist_entity = bp;
		tmp = new BlockInfo(t);
		if (cur == NULL)
		{
			this->freelist_entity = cur = tmp;
		}
		else
		{
			cur->next = tmp;
			cur = tmp;
		}
		fread(&t, sizeof(int), 1, fp);
	}
	fclose(fp);
	fp = NULL;


	fp = fopen(this->free_id_file_literal.c_str(), "r");
	if (fp == NULL)
	{
		cerr << "read literal id info error" << endl;
		return;
	}
	fread(&(this->limitID_literal), sizeof(int), 1, fp);
	fread(&t, sizeof(int), 1, fp);
	while (!feof(fp))
	{
		bp = new BlockInfo(t, this->freelist_literal);
		this->freelist_literal = bp;
		fread(&t, sizeof(int), 1, fp);
	}
	fclose(fp);
	fp = NULL;

	fp = fopen(this->free_id_file_predicate.c_str(), "r");
	if (fp == NULL)
	{
		cerr << "read predicate id info error" << endl;
		return;
	}
	fread(&(this->limitID_predicate), sizeof(int), 1, fp);
	fread(&t, sizeof(int), 1, fp);
	while (!feof(fp))
	{
		bp = new BlockInfo(t, this->freelist_predicate);
		this->freelist_predicate = bp;
		fread(&t, sizeof(int), 1, fp);
	}
	fclose(fp);
	fp = NULL;
}

void
Database::writeIDinfo()
{
	//cout<<"now to write the id info"<<endl;
	FILE* fp = NULL;
	BlockInfo *bp = NULL, *tp = NULL;

	fp = fopen(this->free_id_file_entity.c_str(), "w+");
	if (fp == NULL)
	{
		cerr << "write entity id info error" << endl;
		return;
	}
	fwrite(&(this->limitID_entity), sizeof(int), 1, fp);
	bp = this->freelist_entity;
	while (bp != NULL)
	{
		//if(bp->num == 14912)
		//{
			//cout<<"Database::writeIDinfo() - get 14912"<<endl;
		//}
		fwrite(&(bp->num), sizeof(int), 1, fp);
		tp = bp->next;
		delete bp;
		bp = tp;
	}
	fclose(fp);
	fp = NULL;

	fp = fopen(this->free_id_file_literal.c_str(), "w+");
	if (fp == NULL)
	{
		cerr << "write literal id info error" << endl;
		return;
	}
	fwrite(&(this->limitID_literal), sizeof(int), 1, fp);
	bp = this->freelist_literal;
	while (bp != NULL)
	{
		fwrite(&(bp->num), sizeof(int), 1, fp);
		tp = bp->next;
		delete bp;
		bp = tp;
	}
	fclose(fp);
	fp = NULL;

	fp = fopen(this->free_id_file_predicate.c_str(), "w+");
	if (fp == NULL)
	{
		cerr << "write predicate id info error" << endl;
		return;
	}
	fwrite(&(this->limitID_predicate), sizeof(int), 1, fp);
	bp = this->freelist_predicate;
	while (bp != NULL)
	{
		fwrite(&(bp->num), sizeof(int), 1, fp);
		tp = bp->next;
		delete bp;
		bp = tp;
	}
	fclose(fp);
	fp = NULL;
}

//ID alloc garbage error(LITERAL_FIRST_ID or double) add base for literal
int
Database::allocEntityID()
{
	if (this->freelist_entity == NULL)
	{
#ifdef DEBUG
		//cout<<"Database::allocEntityID() - to double and add"<<endl;
#endif
		//double and add
		int addNum = this->limitID_entity;
		this->limitID_entity *= 2;
		if (this->limitID_entity >= Util::LITERAL_FIRST_ID)
		{
			cerr << "fail to alloc id for entity" << endl;
			return -1;
		}
		BlockInfo* tmp = NULL;
		for (int i = this->limitID_entity - 1; i >= addNum; --i)
		{
			tmp = new BlockInfo(i, this->freelist_entity);
			this->freelist_entity = tmp;
		}
	}
	int t = this->freelist_entity->num;
	//if(t == 14912)
	//{
		//cout<<"Database::allocEntityID() - get 14912"<<endl;
		//cout<<"the next id is "<<this->freelist_entity->next->num<<endl;
	//}
	BlockInfo* op = this->freelist_entity;
	this->freelist_entity = this->freelist_entity->next;
	delete op;

	this->entity_num++;
	return t;
}

void
Database::freeEntityID(int _id)
{
	//if(_id == 14912)
	//{
		//cout<<"Database::freeEntityID() - get 14912"<<endl;
	//}
	BlockInfo* p = new BlockInfo(_id, this->freelist_entity);
	this->freelist_entity = p;

	this->entity_num--;
}

int
Database::allocLiteralID()
{
	if (this->freelist_literal == NULL)
	{
		//double and add
		int addNum = this->limitID_literal;
		this->limitID_literal *= 2;
		if (this->limitID_literal >= Util::LITERAL_FIRST_ID)
		{
			cerr << "fail to alloc id for literal" << endl;
			return -1;
		}
		BlockInfo* tmp = NULL;
		for (int i = this->limitID_literal - 1; i >= addNum; --i)
		{
			tmp = new BlockInfo(i, this->freelist_literal);
			this->freelist_literal = tmp;
		}
	}
	int t = this->freelist_literal->num;
	BlockInfo* op = this->freelist_literal;
	this->freelist_literal = this->freelist_literal->next;
	delete op;

	this->literal_num++;
	return t + Util::LITERAL_FIRST_ID;
}

void
Database::freeLiteralID(int _id)
{
	BlockInfo* p = new BlockInfo(_id - Util::LITERAL_FIRST_ID, this->freelist_literal);
	this->freelist_literal = p;

	this->literal_num--;
}

int
Database::allocPredicateID()
{
	if (this->freelist_predicate == NULL)
	{
		//cout<<"the predicate id list is null"<<endl;
		//double and add
		int addNum = this->limitID_predicate;
		this->limitID_predicate *= 2;
		//cout<<"current limit id "<<this->limitID_predicate<<endl;
		if (addNum >= Util::LITERAL_FIRST_ID)
		{
			cerr << "fail to alloc id for predicate" << endl;
			return -1;
		}
		BlockInfo* tmp = NULL;
		for (int i = this->limitID_predicate - 1; i >= addNum; --i)
		{
			tmp = new BlockInfo(i, this->freelist_predicate);
			//cout<<"loop to add id "<<tmp->num<<endl;
			this->freelist_predicate = tmp;
		}
	}
	//cout<<"get id directly"<<endl;
	int t = this->freelist_predicate->num;
	//cout<<"get num"<<endl;
	BlockInfo* op = this->freelist_predicate;
	this->freelist_predicate = this->freelist_predicate->next;
	//cout<<"transfer to next"<<endl;
	delete op;
	//cout<<"after delete"<<endl;
	//op = NULL;

	this->pre_num++;
	return t;
}

void
Database::freePredicateID(int _id)
{
	BlockInfo* p = new BlockInfo(_id, this->freelist_predicate);
	this->freelist_predicate = p;

	this->pre_num--;
}




void
Database::release(FILE* fp0)
{
	fprintf(fp0, "begin to delete DB!\n");
	fflush(fp0);
	delete this->vstree;
	fprintf(fp0, "ok to delete vstree!\n");
	fflush(fp0);
	delete this->kvstore;
	fprintf(fp0, "ok to delete kvstore!\n");
	fflush(fp0);
	//fclose(Util::debug_database);
	//Util::debug_database = NULL;	//debug: when multiple databases
	fprintf(fp0, "ok to delete DB!\n");
	fflush(fp0);
}

Database::~Database()
{
	this->unload();
	//fclose(Util::debug_database);
	//Util::debug_database = NULL;	//debug: when multiple databases
}

bool
Database::load()
{
	//DEBUG:what if loaded several times?to check if loaded?
	bool flag = (this->vstree)->loadTree();
	if (!flag)
	{
		cerr << "load tree error. @Database::load()" << endl;
		return false;
	}

	flag = this->loadDBInfoFile();
	if (!flag)
	{
		cerr << "load database info error. @Database::load()" << endl;
		return false;
	}

	(this->kvstore)->open();

    this->stringindex->load();
	
	this->readIDinfo();

	stringstream _internal_path;
	_internal_path << this->getStorePath() << "/internal_nodes.dat";
	
	ifstream _internal_input;
	_internal_input.open(_internal_path.str().c_str());
	char* buffer = new char[this->entity_num + 1];
	_internal_input.read(buffer, this->entity_num);
	buffer[this->entity_num] = 0;
	_internal_input.close();
	this->internal_tag_str = string(buffer);
	delete[] buffer;

	return true;
}

bool
Database::loadInternalVertices(const string _in_file)
{
	bool flag = (this->vstree)->loadTree();
	if (!flag)
	{
		cerr << "load tree error. @Database::load()" << endl;
		return false;
	}

	flag = this->loadDBInfoFile();
	if (!flag)
	{
		cerr << "load database info error. @Database::load()" << endl;
		return false;
	}

	(this->kvstore)->open();

    this->stringindex->load();
	
	this->readIDinfo();
	
    string buff;
    ifstream infile;

    infile.open(_in_file.c_str());

    if(!infile){
            cout << "import internal vertices failed." << endl;
    }

    vector<char> buffer(this->entity_num, '0');
    while(getline(infile, buff)){
            buff.erase(0, buff.find_first_not_of("\r\t\n "));
            buff.erase(buff.find_last_not_of("\r\t\n ") + 1);
            //printf("==== %s\n", buff.c_str());
            int _entity_id = (this->kvstore)->getIDByEntity(buff);
            //printf("%d ==== %s\n", _entity_id, buff.c_str());
            buffer[_entity_id] = '1';
            /*
            stringstream _ss;
            _ss << _sub_id;
			string s = _ss.str();
            tp->insert(s.c_str(), strlen(s.c_str()), NULL, 0);
            */
    }

    infile.close();

    stringstream _internal_path;
    _internal_path << this->getStorePath() << "/internal_nodes.dat";
    cout << this->getStorePath() << " begin to import internal vertices to database " << _internal_path.str() << endl;

    //FILE * pFile;
    ofstream _internal_output(_internal_path.str().c_str());
    //pFile = fopen (_internal_path.str().c_str(), "wb");
    //fwrite(buffer, sizeof(char), this->entity_num, pFile);
    //fclose(pFile);
    for (int i = 0; i < buffer.size(); ++i){
            _internal_output << buffer[i];
    }
    _internal_output.close();
	cout << this->getStorePath() << " import internal vertices to database " << _internal_path.str() << " done." << endl;

    return true;
}

bool
Database::unload()
{
	delete this->vstree;
	this->vstree = NULL;
	delete this->kvstore;
	this->kvstore = NULL;
	delete this->stringindex;
	this->stringindex = NULL;

	this->writeIDinfo();
	this->initIDinfo();
	return true;
}

string
Database::getName()
{
	return this->name;
}

bool
Database::query(const string _query, ResultSet& _result_set, vector<string>& partialResStrVec, int myRank, FILE* _fp)
{
    GeneralEvaluation general_evaluation(this->vstree, this->kvstore, this->stringindex);

    long tv_begin = Util::get_cur_time();

	if (!general_evaluation.parseQuery(_query))
		return false;
    long tv_parse = Util::get_cur_time();
	
    //Query
    if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Not_Update)
    {
		if(general_evaluation.getQueryTree().checkStar() == 0){
			general_evaluation.doQuery(this->internal_tag_str);

			//printf("general_evaluation.getLocalPartialResult(this->internal_tag_str, partialResStrVec);\n");
			general_evaluation.getLocalPartialResult(this->kvstore, this->internal_tag_str, partialResStrVec);
		}else{
			general_evaluation.doQuery();
			general_evaluation.getFinalResult(_result_set);
			string final_res_str = _result_set.to_str();
			partialResStrVec.push_back(final_res_str.substr(final_res_str.find("\n") + 1));
		}
    }
    //Update
    else
    {
    	TripleWithObjType *update_triple = NULL;
    	int update_triple_num = 0;

    	if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Data || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Data)
    	{
    		QueryTree::GroupPattern &update_pattern = general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Data ?
    				general_evaluation.getQueryTree().getInsertPatterns() : general_evaluation.getQueryTree().getDeletePatterns();

    		update_triple_num = update_pattern.patterns.size();
    		update_triple  = new TripleWithObjType[update_triple_num];

    		for (int i = 0; i < (int)update_pattern.patterns.size(); i++)
    		{
    			update_triple[i] = TripleWithObjType(	update_pattern.patterns[i].subject.value,
    													update_pattern.patterns[i].predicate.value,
    													update_pattern.patterns[i].object.value);
    		}

    		if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Data)
    		{
    			insert(update_triple, update_triple_num);
    		}
    		else if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Data)
    		{
    			remove(update_triple, update_triple_num);
    		}
    	}
    	else if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Where || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Clause ||
    			general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Clause || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Modify_Clause)
    	{
    		general_evaluation.getQueryTree().setProjectionAsterisk();
    		general_evaluation.doQuery();

    		if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Where || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Clause || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Modify_Clause)
    		{
    			general_evaluation.prepareUpdateTriple(general_evaluation.getQueryTree().getDeletePatterns(), update_triple, update_triple_num);
    			remove(update_triple, update_triple_num);
    		}
    		if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Clause || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Modify_Clause)
    		{
    			general_evaluation.prepareUpdateTriple(general_evaluation.getQueryTree().getInsertPatterns(), update_triple, update_triple_num);
    			insert(update_triple, update_triple_num);
    		}
    	}

    	general_evaluation.releaseResultStack();
    	delete[] update_triple;
    }

#ifdef DEBUG_PRECISE
	fprintf(stderr, "the query function exits!\n");
#endif
	return true;
}

bool
Database::queryCrossingEdge(const string _query, ResultSet& _result_set, vector<string>& lpm_str_vec, vector< vector<int> >& res_crossing_edges_vec, vector<int>& all_crossing_edges_vec, int myRank, FILE* _fp)
{
    GeneralEvaluation general_evaluation(this->vstree, this->kvstore, this->stringindex);

    long tv_begin = Util::get_cur_time();

	if (!general_evaluation.parseQuery(_query))
		return false;
    long tv_parse = Util::get_cur_time();
	
    //Query
    if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Not_Update)
    {
		if(general_evaluation.getQueryTree().checkStar() == 0){
			general_evaluation.doQuery(this->internal_tag_str);

			//printf("general_evaluation.getLocalPartialResult(this->internal_tag_str, partialResStrVec);\n");
			general_evaluation.getCrossingEdges(this->kvstore, this->internal_tag_str, lpm_str_vec, res_crossing_edges_vec, all_crossing_edges_vec);
		}else{
			general_evaluation.doQuery();
			general_evaluation.getFinalResult(_result_set);
			string final_res_str = _result_set.to_str();
			lpm_str_vec.push_back(final_res_str.substr(final_res_str.find("\n") + 1));
		}
    }

	return true;
}


int
Database::queryPathBMC(const string _query, ResultSet& _result_set, string& final_res_str, int myRank, FILE* _fp)
{
    GeneralEvaluation general_evaluation(this->vstree, this->kvstore, this->stringindex);

    long tv_begin = Util::get_cur_time();

	if (!general_evaluation.parseQuery(_query))
		return false;
    long tv_parse = Util::get_cur_time();
	
    //Query
    if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Not_Update)
    {
		general_evaluation.doQuery();
		general_evaluation.getFinalResult(_result_set);
		final_res_str = _result_set.to_str();
		//res_str_vec.push_back(final_res_str.substr(final_res_str.find("\n") + 1));
	}

	return _result_set.ansNum;
}

int
Database::generateCandidate(const string _query, vector< vector<int> >& candidates_vec, vector< vector<int> > &_query_dir_ad, vector< vector<int> > &_query_pre_ad, vector< vector<int> > &_query_ad, set<int> &satellites_set, ResultSet& _result_set, vector<string>& lpm_str_vec, vector< vector<int> >& candidate_id_vec)
{
    GeneralEvaluation general_evaluation(this->vstree, this->kvstore, this->stringindex);

    long tv_begin = Util::get_cur_time();

	if (!general_evaluation.parseQuery(_query))
		return -1;
    long tv_parse = Util::get_cur_time();
	
    //Query
    if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Not_Update)
    {
		//0 means that the query is not star, and 1 means that the query is a star
		if(general_evaluation.getQueryTree().checkStar() == 0){
			general_evaluation.findCandidate(this->internal_tag_str, candidates_vec, candidate_id_vec, _query_dir_ad, _query_pre_ad, _query_ad, satellites_set);
		}else{
			general_evaluation.doQuery();
			general_evaluation.getFinalResult(_result_set);
			string final_res_str = _result_set.to_str();
			lpm_str_vec.push_back(final_res_str.substr(final_res_str.find("\n") + 1));
			return 1;
		}
    }

	return 0;
}

bool
Database::query(const string _query, ResultSet& _result_set, FILE* _fp)
{
    GeneralEvaluation general_evaluation(this->vstree, this->kvstore, this->stringindex);

    long tv_begin = Util::get_cur_time();

	if (!general_evaluation.parseQuery(_query))
		return false;
    long tv_parse = Util::get_cur_time();
    cout << "after Parsing, used " << (tv_parse - tv_begin) << "ms." << endl;

    //Query
    if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Not_Update)
    {
    	general_evaluation.doQuery();

        long tv_bfget = Util::get_cur_time();
        general_evaluation.getFinalResult(_result_set);
        long tv_afget = Util::get_cur_time();
        cout << "after getFinalResult, used " << (tv_afget - tv_bfget) << "ms." << endl;

        general_evaluation.setNeedOutputAnswer();
    }
    //Update
    else
    {
    	TripleWithObjType *update_triple = NULL;
    	int update_triple_num = 0;

    	if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Data || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Data)
    	{
    		QueryTree::GroupPattern &update_pattern = general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Data ?
    				general_evaluation.getQueryTree().getInsertPatterns() : general_evaluation.getQueryTree().getDeletePatterns();

    		update_triple_num = update_pattern.patterns.size();
    		update_triple  = new TripleWithObjType[update_triple_num];

    		for (int i = 0; i < (int)update_pattern.patterns.size(); i++)
    		{
    			update_triple[i] = TripleWithObjType(	update_pattern.patterns[i].subject.value,
    													update_pattern.patterns[i].predicate.value,
    													update_pattern.patterns[i].object.value);
    		}

    		if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Data)
    		{
    			insert(update_triple, update_triple_num);
    		}
    		else if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Data)
    		{
    			remove(update_triple, update_triple_num);
    		}
    	}
    	else if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Where || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Clause ||
    			general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Clause || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Modify_Clause)
    	{
    		general_evaluation.getQueryTree().setProjectionAsterisk();
    		general_evaluation.doQuery();

    		if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Where || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Delete_Clause || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Modify_Clause)
    		{
    			general_evaluation.prepareUpdateTriple(general_evaluation.getQueryTree().getDeletePatterns(), update_triple, update_triple_num);
    			remove(update_triple, update_triple_num);
    		}
    		if (general_evaluation.getQueryTree().getUpdateType() == QueryTree::Insert_Clause || general_evaluation.getQueryTree().getUpdateType() == QueryTree::Modify_Clause)
    		{
    			general_evaluation.prepareUpdateTriple(general_evaluation.getQueryTree().getInsertPatterns(), update_triple, update_triple_num);
    			insert(update_triple, update_triple_num);
    		}
    	}

    	general_evaluation.releaseResultStack();
    	delete[] update_triple;
    }

    long tv_final = Util::get_cur_time();
    cout << "Total time used: " << (tv_final - tv_begin) << "ms." << endl;

	if (general_evaluation.needOutputAnswer())
	{
		cout << "There has answer: " << _result_set.ansNum << endl;
		cout << "final result is : " << endl;
		if (!_result_set.checkUseStream())
		{

			cout << _result_set.to_str() << endl;
		}
		else
		{
			_result_set.output(_fp);
			//cout<<endl;		//empty the buffer;print an empty line
			fprintf(_fp, "\n");
			fflush(_fp);       //to empty the output buffer in C (fflush(stdin) not work in GCC)
		}
	}

#ifdef DEBUG_PRECISE
	fprintf(stderr, "the query function exits!\n");
#endif
	return true;
}

//NOTICE+QUERY:to save memory for large cases, we can consider building one tree at a time(then release)
//Or read the rdf file on separate segments
//WARN:the ID type is int, and entity/literal are just separated by a limit
//which means that entity num <= 10^9 literal num <= 10^9 predicate num <= 2*10^9
//If we use unsigned as type, then max triple can be 10^9(edge case)
//If we use long long, no more problem, but wasteful
//Or we can consider divide entity and literal totally
//In distributed gStore, each machine's graph should be based on unique encoding IDs,
//and require that triples in each graph no more than a limit(maybe 10^9)
bool
Database::build(const string& _rdf_file)
{
	//manage the id for a new database
	this->resetIDinfo();

	string ret = Util::getExactPath(_rdf_file.c_str());
	long tv_build_begin = Util::get_cur_time();

	string store_path = this->name;
	Util::create_dir(store_path);

	string kv_store_path = store_path + "/kv_store";
	Util::create_dir(kv_store_path);

	string vstree_store_path = store_path + "/vs_store";
	Util::create_dir(vstree_store_path);
	
	string stringindex_store_path = store_path + "/stringindex_store";
    Util::create_dir(stringindex_store_path);

	cout << "begin encode RDF from : " << ret << " ..." << endl;

	//BETTER+TODO:now require that dataset size < memory
	//to support really larger datasets, divide and insert into B+ tree and VStree
	//(read the value, add list and set; update the signature, remove and reinsert)
	//the query process is nearly the same
	//QUERY:build each index in one thread to speed up, but the sorting case?
	//one sorting order for several indexes

	// to be switched to new encodeRDF method.
	//    this->encodeRDF(ret);
	if (!this->encodeRDF_new(ret))	//<-- this->kvstore->id2* trees are closed
	{
		return false;
	}
	cout << "finish encode." << endl;

	//cout<<"test kv"<<this->kvstore->getIDByPredicate("<contain>")<<endl;

	//this->kvstore->flush();
	delete this->kvstore;
	this->kvstore = NULL;
	//sync();
	//cout << "sync kvstore" << endl;
	//this->kvstore->release();
	//cout<<"release kvstore"<<endl;

	//(this->kvstore)->open();
	string _entry_file = this->getSignatureBFile();

	cout << "begin build VS-Tree on " << ret << "..." << endl;
	(this->vstree)->buildTree(_entry_file);

	long tv_build_end = Util::get_cur_time();
	cout << "after build, used " << (tv_build_end - tv_build_begin) << "ms." << endl;
	cout << "finish build VS-Tree." << endl;
	
	delete this->vstree;
	this->vstree = NULL;
	//sync();
	//cout << "sync vstree" << endl;

	//string cmd = "rm -rf " + _entry_file;
	//system(cmd.c_str());
	//cout << "signature file removed" << endl;

	return true;
}

//NOTICE+QUERY:to save memory for large cases, we can consider building one tree at a time(then release)
//Or read the rdf file on separate segments
//WARN:the ID type is int, and entity/literal are just separated by a limit
//which means that entity num <= 10^9 literal num <= 10^9 predicate num <= 2*10^9
//If we use unsigned as type, then max triple can be 10^9(edge case)
//If we use long long, no more problem, but wasteful
//Or we can consider divide entity and literal totally
//In distributed gStore, each machine's graph should be based on unique encoding IDs,
//and require that triples in each graph no more than a limit(maybe 10^9)
bool
Database::build(const string& _rdf_file, const char* _internal_file)
{
	//manage the id for a new database
	this->resetIDinfo();

	string ret = Util::getExactPath(_rdf_file.c_str());
	long tv_build_begin = Util::get_cur_time();

	string store_path = this->name;
	Util::create_dir(store_path);

	string kv_store_path = store_path + "/kv_store";
	Util::create_dir(kv_store_path);

	string vstree_store_path = store_path + "/vs_store";
	Util::create_dir(vstree_store_path);
	
	string stringindex_store_path = store_path + "/stringindex_store";
    Util::create_dir(stringindex_store_path);

	cout << "begin encode RDF from : " << ret << " ..." << endl;

	//BETTER+TODO:now require that dataset size < memory
	//to support really larger datasets, divide and insert into B+ tree and VStree
	//(read the value, add list and set; update the signature, remove and reinsert)
	//the query process is nearly the same
	//QUERY:build each index in one thread to speed up, but the sorting case?
	//one sorting order for several indexes

	// to be switched to new encodeRDF method.
	//    this->encodeRDF(ret);
	if (!this->encodeRDF_new(ret, _internal_file))	//<-- this->kvstore->id2* trees are closed
	{
		return false;
	}
	cout << "finish encode." << endl;

	//this->kvstore->flush();
	delete this->kvstore;
	this->kvstore = NULL;
	//sync();
	//cout << "sync kvstore" << endl;
	//this->kvstore->release();
	//cout<<"release kvstore"<<endl;

	//(this->kvstore)->open();
	string _entry_file = this->getSignatureBFile();

	cout << "begin build VS-Tree on " << ret << "..." << endl;
	(this->vstree)->buildTree(_entry_file);

	long tv_build_end = Util::get_cur_time();
	cout << "after build, used " << (tv_build_end - tv_build_begin) << "ms." << endl;
	cout << "finish build VS-Tree." << endl;
	
	delete this->vstree;
	this->vstree = NULL;
	//sync();
	//cout << "sync vstree" << endl;

	//string cmd = "rm -rf " + _entry_file;
	//system(cmd.c_str());
	//cout << "signature file removed" << endl;

	return true;
}

//root Path of this DB + sixTuplesFile
string
Database::getSixTuplesFile()
{
	return this->getStorePath() + "/" + this->six_tuples_file;
}

/* root Path of this DB + signatureBFile */
string
Database::getSignatureBFile()
{
	return this->getStorePath() + "/" + this->signature_binary_file;
}

/* root Path of this DB + DBInfoFile */
string
Database::getDBInfoFile()
{
	return this->getStorePath() + "/" + this->db_info_file;
}

bool
Database::saveDBInfoFile()
{
	FILE* filePtr = fopen(this->getDBInfoFile().c_str(), "wb");

	if (filePtr == NULL)
	{
		cerr << "error, can not create db info file. @Database::saveDBInfoFile" << endl;
		return false;
	}

	fseek(filePtr, 0, SEEK_SET);

	fwrite(&this->triples_num, sizeof(int), 1, filePtr);
	fwrite(&this->entity_num, sizeof(int), 1, filePtr);
	fwrite(&this->sub_num, sizeof(int), 1, filePtr);
	fwrite(&this->pre_num, sizeof(int), 1, filePtr);
	fwrite(&this->literal_num, sizeof(int), 1, filePtr);
	fwrite(&this->encode_mode, sizeof(int), 1, filePtr);
	fclose(filePtr);

	Util::triple_num = this->triples_num;
	Util::pre_num = this->pre_num;
	Util::entity_num = this->entity_num;
	Util::literal_num = this->literal_num;

	return true;
}

bool
Database::loadDBInfoFile()
{
	FILE* filePtr = fopen(this->getDBInfoFile().c_str(), "rb");

	if (filePtr == NULL)
	{
		cerr << "error, can not open db info file. @Database::loadDBInfoFile" << endl;
		return false;
	}

	fseek(filePtr, 0, SEEK_SET);

	fread(&this->triples_num, sizeof(int), 1, filePtr);
	fread(&this->entity_num, sizeof(int), 1, filePtr);
	fread(&this->sub_num, sizeof(int), 1, filePtr);
	fread(&this->pre_num, sizeof(int), 1, filePtr);
	fread(&this->literal_num, sizeof(int), 1, filePtr);
	fread(&this->encode_mode, sizeof(int), 1, filePtr);
	fclose(filePtr);

	Util::triple_num = this->triples_num;
	Util::pre_num = this->pre_num;
	Util::entity_num = this->entity_num;
	Util::literal_num = this->literal_num;

	return true;
}

string
Database::getStorePath()
{
	return this->name;
}


//encode relative signature data of the query graph
void
Database::buildSparqlSignature(SPARQLquery & _sparql_q)
{
	vector<BasicQuery*>& _query_union = _sparql_q.getBasicQueryVec();
	for (unsigned int i_bq = 0; i_bq < _query_union.size(); i_bq++)
	{
		BasicQuery* _basic_q = _query_union[i_bq];
		_basic_q->encodeBasicQuery(this->kvstore, _sparql_q.getQueryVar());
	}
}

bool
Database::calculateEntityBitSet(int _entity_id, EntityBitSet & _bitset)
{
	int _list_len = 0;
	//when as subject
	int* _polist = NULL;
	(this->kvstore)->getpreIDobjIDlistBysubID(_entity_id, _polist, _list_len);
	Triple _triple;
	_triple.subject = (this->kvstore)->getEntityByID(_entity_id);
	for (int i = 0; i < _list_len; i += 2)
	{
		int _pre_id = _polist[i];
		int _obj_id = _polist[i + 1];
		_triple.object = (this->kvstore)->getEntityByID(_obj_id);
		if (_triple.object == "")
		{
			_triple.object = (this->kvstore)->getLiteralByID(_obj_id);
		}
		_triple.predicate = (this->kvstore)->getPredicateByID(_pre_id);
		this->encodeTriple2SubEntityBitSet(_bitset, &_triple);
	}
	delete[] _polist;

	//when as object
	int* _pslist = NULL;
	_list_len = 0;
	(this->kvstore)->getpreIDsubIDlistByobjID(_entity_id, _pslist, _list_len);
	_triple.object = (this->kvstore)->getEntityByID(_entity_id);
	for (int i = 0; i < _list_len; i += 2)
	{
		int _pre_id = _pslist[i];
		int _sub_id = _pslist[i + 1];
		_triple.subject = (this->kvstore)->getEntityByID(_sub_id);
		_triple.predicate = (this->kvstore)->getPredicateByID(_pre_id);
		this->encodeTriple2ObjEntityBitSet(_bitset, &_triple);
	}
	delete[] _pslist;

	return true;
}

//encode Triple into subject SigEntry
bool
Database::encodeTriple2SubEntityBitSet(EntityBitSet& _bitset, const Triple* _p_triple)
{
	int _pre_id = -1;
	{
		_pre_id = (this->kvstore)->getIDByPredicate(_p_triple->predicate);
		//BETTER: checking whether _pre_id is -1 or not will be more reliable
	}

	Signature::encodePredicate2Entity(_pre_id, _bitset, Util::EDGE_OUT);
	if (this->encode_mode == Database::ID_MODE)
	{
		//TODO
	}
	else if (this->encode_mode == Database::STRING_MODE)
	{
		Signature::encodeStr2Entity((_p_triple->object).c_str(), _bitset);
	}

	return true;
}

//encode Triple into object SigEntry
bool
Database::encodeTriple2ObjEntityBitSet(EntityBitSet& _bitset, const Triple* _p_triple)
{
	int _pre_id = -1;
	{
		_pre_id = (this->kvstore)->getIDByPredicate(_p_triple->predicate);
		//BETTER: checking whether _pre_id is -1 or not will be more reliable
	}

	Signature::encodePredicate2Entity(_pre_id, _bitset, Util::EDGE_IN);
	if (this->encode_mode == Database::ID_MODE)
	{
		//TODO
	}
	else if (this->encode_mode == Database::STRING_MODE)
	{
		Signature::encodeStr2Entity((_p_triple->subject).c_str(), _bitset);
	}

	return true;
}

//check whether the relative 3-tuples exist usually, through sp2olist
bool
Database::exist_triple(int _sub_id, int _pre_id, int _obj_id)
{
	int* _objidlist = NULL;
	int _list_len = 0;
	(this->kvstore)->getobjIDlistBysubIDpreID(_sub_id, _pre_id, _objidlist, _list_len);

	bool is_exist = false;
	//	for(int i = 0; i < _list_len; i ++)
	//	{
	//		if(_objidlist[i] == _obj_id)
	//		{
	//		    is_exist = true;
	//			break;
	//		}
	//	}
	if (Util::bsearch_int_uporder(_obj_id, _objidlist, _list_len) != -1)
	{
		is_exist = true;
	}
	delete[] _objidlist;

	return is_exist;
}

//NOTICE: all constants are transfered to ids in memory
//this maybe not ok when size is too large!
bool
Database::encodeRDF_new(const string _rdf_file, const char* _in_file)
{
#ifdef DEBUG
	//cerr<< "now to log!!!" << endl;
	Util::logging("In encodeRDF_new");
	//cerr<< "end log!!!" << endl;
#endif
	int** _p_id_tuples = NULL;
	int _id_tuples_max = 0;

	//map sub2id, pre2id, entity/literal in obj2id, store in kvstore, encode RDF data into signature
	if (!this->sub2id_pre2id_obj2id_RDFintoSignature(_rdf_file, _p_id_tuples, _id_tuples_max, _in_file))
	{
		return false;
	}

    //build stringindex before this->kvstore->id2* trees are closed
    this->stringindex->setNum(StringIndexFile::Entity, this->entity_num);
    this->stringindex->setNum(StringIndexFile::Literal, this->literal_num);
    this->stringindex->setNum(StringIndexFile::Predicate, this->pre_num);
    this->stringindex->save(*this->kvstore);

	//NOTICE:close these trees now to save memory
	this->kvstore->close_entity2id();
	this->kvstore->close_id2entity();
	this->kvstore->close_literal2id();
	this->kvstore->close_id2literal();
	this->kvstore->close_predicate2id();
	this->kvstore->close_id2predicate();

	/* map subid 2 objid_list  &
	* subIDpreID 2 objid_list &
	* subID 2 <preIDobjID>_list */
	//this->s2o_s2po_sp2o(_p_id_tuples, _id_tuples_max);

	/* map objid 2 subid_list  &
	* objIDpreID 2 subid_list &
	* objID 2 <preIDsubID>_list */
	//this->o2s_o2ps_op2s(_p_id_tuples, _id_tuples_max);

	//this->s2p_s2po_sp2o(_p_id_tuples, _id_tuples_max);
	//NOTICE:we had better compute the corresponding triple num here
	this->s2p_s2o_s2po_sp2o(_p_id_tuples, _id_tuples_max);
	//this->s2p_s2o_s2po_sp2o_sp2n(_p_id_tuples, _id_tuples_max);
	this->o2p_o2s_o2ps_op2s(_p_id_tuples, _id_tuples_max);
	//this->o2p_o2s_o2ps_op2s_op2n(_p_id_tuples, _id_tuples_max);
	this->p2s_p2o_p2so(_p_id_tuples, _id_tuples_max);
	//this->p2s_p2o_p2so_p2n(_p_id_tuples, _id_tuples_max);
	//
	//WARN:this is too costly because s-o key num is too large
	//100G+ for DBpedia2014
	//this->so2p_s2o(_p_id_tuples, _id_tuples_max);

	//WARN:we must free the memory for id_tuples array
	//for (int i = 0; i < _id_tuples_max; ++i)
	for (int i = 0; i < this->triples_num; ++i)
	{
		delete[] _p_id_tuples[i];
	}
	delete[] _p_id_tuples;

	bool flag = this->saveDBInfoFile();
	if (!flag)
	{
		return false;
	}

	Util::logging("finish encodeRDF_new");

	return true;
}

//NOTICE: all constants are transfered to ids in memory
//this maybe not ok when size is too large!
bool
Database::encodeRDF_new(const string _rdf_file)
{
#ifdef DEBUG
	//cerr<< "now to log!!!" << endl;
	Util::logging("In encodeRDF_new");
	//cerr<< "end log!!!" << endl;
#endif
	int** _p_id_tuples = NULL;
	int _id_tuples_max = 0;

	//map sub2id, pre2id, entity/literal in obj2id, store in kvstore, encode RDF data into signature
	if (!this->sub2id_pre2id_obj2id_RDFintoSignature(_rdf_file, _p_id_tuples, _id_tuples_max))
	{
		return false;
	}

    //build stringindex before this->kvstore->id2* trees are closed
    this->stringindex->setNum(StringIndexFile::Entity, this->entity_num);
    this->stringindex->setNum(StringIndexFile::Literal, this->literal_num);
    this->stringindex->setNum(StringIndexFile::Predicate, this->pre_num);
    this->stringindex->save(*this->kvstore);

	//NOTICE:close these trees now to save memory
	this->kvstore->close_entity2id();
	this->kvstore->close_id2entity();
	this->kvstore->close_literal2id();
	this->kvstore->close_id2literal();
	this->kvstore->close_predicate2id();
	this->kvstore->close_id2predicate();

	/* map subid 2 objid_list  &
	* subIDpreID 2 objid_list &
	* subID 2 <preIDobjID>_list */
	//this->s2o_s2po_sp2o(_p_id_tuples, _id_tuples_max);

	/* map objid 2 subid_list  &
	* objIDpreID 2 subid_list &
	* objID 2 <preIDsubID>_list */
	//this->o2s_o2ps_op2s(_p_id_tuples, _id_tuples_max);

	//this->s2p_s2po_sp2o(_p_id_tuples, _id_tuples_max);
	//NOTICE:we had better compute the corresponding triple num here
	this->s2p_s2o_s2po_sp2o(_p_id_tuples, _id_tuples_max);
	//this->s2p_s2o_s2po_sp2o_sp2n(_p_id_tuples, _id_tuples_max);
	this->o2p_o2s_o2ps_op2s(_p_id_tuples, _id_tuples_max);
	//this->o2p_o2s_o2ps_op2s_op2n(_p_id_tuples, _id_tuples_max);
	this->p2s_p2o_p2so(_p_id_tuples, _id_tuples_max);
	//this->p2s_p2o_p2so_p2n(_p_id_tuples, _id_tuples_max);
	//
	//WARN:this is too costly because s-o key num is too large
	//100G+ for DBpedia2014
	//this->so2p_s2o(_p_id_tuples, _id_tuples_max);

	//WARN:we must free the memory for id_tuples array
	//for (int i = 0; i < _id_tuples_max; ++i)
	for (int i = 0; i < this->triples_num; ++i)
	{
		delete[] _p_id_tuples[i];
	}
	delete[] _p_id_tuples;

	bool flag = this->saveDBInfoFile();
	if (!flag)
	{
		return false;
	}

	Util::logging("finish encodeRDF_new");

	return true;
}


bool
Database::sub2id_pre2id_obj2id_RDFintoSignature(const string _rdf_file, int**& _p_id_tuples, int & _id_tuples_max, const char* _in_file)
{
	int _id_tuples_size;
	{
		//initial
		_id_tuples_max = 10 * 1000 * 1000;
		_p_id_tuples = new int*[_id_tuples_max];
		_id_tuples_size = 0;
		this->sub_num = 0;
		this->pre_num = 0;
		this->entity_num = 0;
		this->literal_num = 0;
		this->triples_num = 0;
		(this->kvstore)->open_entity2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2entity(KVstore::CREATE_MODE);
		(this->kvstore)->open_predicate2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2predicate(KVstore::CREATE_MODE);
		(this->kvstore)->open_literal2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2literal(KVstore::CREATE_MODE);
	}

	//Util::logging("finish initial sub2id_pre2id_obj2id");
	cerr << "finish initial sub2id_pre2id_obj2id" << endl;

	ifstream _fin(_rdf_file.c_str());
	if (!_fin)
	{
		cerr << "sub2id&pre2id&obj2id: Fail to open : " << _rdf_file << endl;
		//exit(0);
		return false;
	}

	string _six_tuples_file = this->getSixTuplesFile();
	ofstream _six_tuples_fout(_six_tuples_file.c_str());
	if (!_six_tuples_fout)
	{
		cerr << "sub2id&pre2id&obj2id: Fail to open: " << _six_tuples_file << endl;
		//exit(0);
		return false;
	}

	TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];

	//don't know the number of entity
	//pre allocate entitybitset_max EntityBitSet for storing signature, double the space until the _entity_bitset is used up.
	int entitybitset_max = 10000;
	EntityBitSet** _entity_bitset = new EntityBitSet*[entitybitset_max];
	for (int i = 0; i < entitybitset_max; i++)
	{
		_entity_bitset[i] = new EntityBitSet();
		_entity_bitset[i]->reset();
	}
	EntityBitSet _tmp_bitset;

	//parse a file
	RDFParser _parser(_fin);

	Util::logging("==> while(true)");

	while (true)
	{
		int parse_triple_num = 0;

		_parser.parseFile(triple_array, parse_triple_num);

		{
			stringstream _ss;
			_ss << "finish rdfparser" << this->triples_num << endl;
			//Util::logging(_ss.str());
			cout << _ss.str() << endl;
		}
		cout<<"after info in sub2id_"<<endl;

		if (parse_triple_num == 0)
		{
			break;
		}

		//Process the Triple one by one
		for (int i = 0; i < parse_triple_num; i++)
		{
			this->triples_num++;

			//if the _id_tuples exceeds, double the space
			if (_id_tuples_size == _id_tuples_max)
			{
				int _new_tuples_len = _id_tuples_max * 2;
				int** _new_id_tuples = new int*[_new_tuples_len];
				memcpy(_new_id_tuples, _p_id_tuples, sizeof(int*) * _id_tuples_max);
				delete[] _p_id_tuples;
				_p_id_tuples = _new_id_tuples;
				_id_tuples_max = _new_tuples_len;
			}

			// For subject
			// (all subject is entity, some object is entity, the other is literal)
			string _sub = triple_array[i].getSubject();
			int _sub_id = (this->kvstore)->getIDByEntity(_sub);
			if (_sub_id == -1)
			{
				//_sub_id = this->entity_num;
				_sub_id = this->allocEntityID();
				//this->entity_num++;
				(this->kvstore)->setIDByEntity(_sub, _sub_id);
				(this->kvstore)->setEntityByID(_sub_id, _sub);
			}
			//  For predicate
			string _pre = triple_array[i].getPredicate();
			int _pre_id = (this->kvstore)->getIDByPredicate(_pre);
			if (_pre_id == -1)
			{
				//_pre_id = this->pre_num;
				_pre_id = this->allocPredicateID();
				//this->pre_num++;
				(this->kvstore)->setIDByPredicate(_pre, _pre_id);
				(this->kvstore)->setPredicateByID(_pre_id, _pre);
			}

			//  For object
			string _obj = triple_array[i].getObject();
			int _obj_id = -1;
			// obj is entity
			if (triple_array[i].isObjEntity())
			{
				_obj_id = (this->kvstore)->getIDByEntity(_obj);
				if (_obj_id == -1)
				{
					//_obj_id = this->entity_num;
					_obj_id = this->allocEntityID();
					//this->entity_num++;
					(this->kvstore)->setIDByEntity(_obj, _obj_id);
					(this->kvstore)->setEntityByID(_obj_id, _obj);
				}
			}
			//obj is literal
			if (triple_array[i].isObjLiteral())
			{
				_obj_id = (this->kvstore)->getIDByLiteral(_obj);
				if (_obj_id == -1)
				{
					//_obj_id = Util::LITERAL_FIRST_ID + (this->literal_num);
					_obj_id = this->allocLiteralID();
					//this->literal_num++;
					(this->kvstore)->setIDByLiteral(_obj, _obj_id);
					(this->kvstore)->setLiteralByID(_obj_id, _obj);
					//#ifdef DEBUG
					//if(_obj == "\"Bob\"")
					//{
					//cout << "this is id for Bob: " << _obj_id << endl;
					//}
					//cout<<"literal should be bob: " << kvstore->getLiteralByID(_obj_id)<<endl;
					//cout<<"id for bob: "<<kvstore->getIDByLiteral("\"Bob\"")<<endl;
					//#endif
				}
			}

			//  For id_tuples
			_p_id_tuples[_id_tuples_size] = new int[3];
			_p_id_tuples[_id_tuples_size][0] = _sub_id;
			_p_id_tuples[_id_tuples_size][1] = _pre_id;
			_p_id_tuples[_id_tuples_size][2] = _obj_id;
			_id_tuples_size++;

#ifdef DEBUG_PRECISE
			//  save six tuples
			{
				_six_tuples_fout << _sub_id << '\t'
					<< _pre_id << '\t'
					<< _obj_id << '\t'
					<< _sub << '\t'
					<< _pre << '\t'
					<< _obj << endl;
			}
#endif
			//_entity_bitset is used up, double the space
			if (this->entity_num >= entitybitset_max)
			{
				//cout<<"to double entity bitset num"<<endl;
				EntityBitSet** _new_entity_bitset = new EntityBitSet*[entitybitset_max * 2];
				memcpy(_new_entity_bitset, _entity_bitset, sizeof(EntityBitSet*) * entitybitset_max);
				delete[] _entity_bitset;
				_entity_bitset = _new_entity_bitset;

				int tmp = entitybitset_max * 2;
				for (int i = entitybitset_max; i < tmp; i++)
				{
					_entity_bitset[i] = new EntityBitSet();
					_entity_bitset[i]->reset();
				}

				entitybitset_max = tmp;
			}

			{
				_tmp_bitset.reset();
				Signature::encodePredicate2Entity(_pre_id, _tmp_bitset, Util::EDGE_OUT);
				Signature::encodeStr2Entity(_obj.c_str(), _tmp_bitset);
				*_entity_bitset[_sub_id] |= _tmp_bitset;
			}

			if (triple_array[i].isObjEntity())
			{
				_tmp_bitset.reset();
				Signature::encodePredicate2Entity(_pre_id, _tmp_bitset, Util::EDGE_IN);
				Signature::encodeStr2Entity(_sub.c_str(), _tmp_bitset);
				//cout<<"objid: "<<_obj_id <<endl;
				//when 15999 error
				//WARN:id allocated can be very large while the num is not so much
				//This means that maybe the space for the _obj_id is not allocated now
				//NOTICE:we prepare the free id list in uporder and contiguous, so in build process
				//this can work well
				*_entity_bitset[_obj_id] |= _tmp_bitset;
			}
		}
	}

	Util::logging("==> end while(true)");

	delete[] triple_array;
	_fin.close();
	_six_tuples_fout.close();
	
	//-------------------------------- begin loading internal uris --------------------------------
	vector<char> buffer(this->entity_num, '0');
	string buff;
    ifstream infile;

    infile.open(_in_file);

    if(!infile){
            cout << "import internal vertices failed." << endl;
    }
	set<string> internal_set;
    
    while(getline(infile, buff)){
            buff.erase(0, buff.find_first_not_of("\r\t\n "));
            buff.erase(buff.find_last_not_of("\r\t\n ") + 1);
            internal_set.insert(buff);
    }

    infile.close();
	//-------------------------------- finish loading internal uris --------------------------------

	{
		//save all entity_signature into binary file
		string _sig_binary_file = this->getSignatureBFile();
		FILE* _sig_fp = fopen(_sig_binary_file.c_str(), "wb");
		if (_sig_fp == NULL) {
			cerr << "Failed to open : " << _sig_binary_file << endl;
		}

		//EntityBitSet _all_bitset;
		for (int i = 0; i < this->entity_num; i++)
		{
			SigEntry* _sig = new SigEntry(EntitySig(*_entity_bitset[i]), i);
			fwrite(_sig, sizeof(SigEntry), 1, _sig_fp);
			//_all_bitset |= *_entity_bitset[i];
			
			string tmp_uri_str = this->kvstore->getEntityByID(i);
			if(internal_set.count(tmp_uri_str) != 0){
				buffer[i] = '1';
			}
			
			delete _sig;
		}
		fclose(_sig_fp);

		for (int i = 0; i < entitybitset_max; i++)
		{
			delete _entity_bitset[i];
		}
		delete[] _entity_bitset;
	}
	
	//-------------------------------- begin writing internal vertices --------------------------------
	stringstream _internal_path;
    _internal_path << this->getStorePath() << "/internal_nodes.dat";
    cout << this->getStorePath() << " begin to import internal vertices to database " << _internal_path.str() << endl;

    //FILE * pFile;
    ofstream _internal_output(_internal_path.str().c_str());
    //pFile = fopen (_internal_path.str().c_str(), "wb");
    //fwrite(buffer, sizeof(char), this->entity_num, pFile);
    //fclose(pFile);
    for (int i = 0; i < buffer.size(); ++i){
            _internal_output << buffer[i];
    }
    _internal_output.close();
	cout << this->getStorePath() << " import internal vertices to database " << _internal_path.str() << " done." << endl;
	//-------------------------------- finish writing internal vertices --------------------------------

	{
		stringstream _ss;
		_ss << "finish sub2id pre2id obj2id" << endl;
		_ss << "tripleNum is " << this->triples_num << endl;
		_ss << "entityNum is " << this->entity_num << endl;
		_ss << "preNum is " << this->pre_num << endl;
		_ss << "literalNum is " << this->literal_num << endl;
		Util::logging(_ss.str());
		cout << _ss.str() << endl;
	}

	return true;
}

//NOTICE: below are the the new ones

bool
Database::sub2id_pre2id_obj2id_RDFintoSignature(const string _rdf_file, int**& _p_id_tuples, int & _id_tuples_max)
{
	int _id_tuples_size;
	{
		//initial
		_id_tuples_max = 10 * 1000 * 1000;
		_p_id_tuples = new int*[_id_tuples_max];
		_id_tuples_size = 0;
		this->sub_num = 0;
		this->pre_num = 0;
		this->entity_num = 0;
		this->literal_num = 0;
		this->triples_num = 0;
		(this->kvstore)->open_entity2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2entity(KVstore::CREATE_MODE);
		(this->kvstore)->open_predicate2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2predicate(KVstore::CREATE_MODE);
		(this->kvstore)->open_literal2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2literal(KVstore::CREATE_MODE);
	}

	//Util::logging("finish initial sub2id_pre2id_obj2id");
	cerr << "finish initial sub2id_pre2id_obj2id" << endl;

	ifstream _fin(_rdf_file.c_str());
	if (!_fin)
	{
		cerr << "sub2id&pre2id&obj2id: Fail to open : " << _rdf_file << endl;
		//exit(0);
		return false;
	}

	string _six_tuples_file = this->getSixTuplesFile();
	ofstream _six_tuples_fout(_six_tuples_file.c_str());
	if (!_six_tuples_fout)
	{
		cerr << "sub2id&pre2id&obj2id: Fail to open: " << _six_tuples_file << endl;
		//exit(0);
		return false;
	}

	TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];

	//don't know the number of entity
	//pre allocate entitybitset_max EntityBitSet for storing signature, double the space until the _entity_bitset is used up.
	int entitybitset_max = 10000;
	EntityBitSet** _entity_bitset = new EntityBitSet*[entitybitset_max];
	for (int i = 0; i < entitybitset_max; i++)
	{
		_entity_bitset[i] = new EntityBitSet();
		_entity_bitset[i]->reset();
	}
	EntityBitSet _tmp_bitset;

	//parse a file
	RDFParser _parser(_fin);

	Util::logging("==> while(true)");

	while (true)
	{
		int parse_triple_num = 0;

		_parser.parseFile(triple_array, parse_triple_num);

		{
			stringstream _ss;
			_ss << "finish rdfparser" << this->triples_num << endl;
			//Util::logging(_ss.str());
			cout << _ss.str() << endl;
		}
		cout<<"after info in sub2id_"<<endl;

		if (parse_triple_num == 0)
		{
			break;
		}

		//Process the Triple one by one
		for (int i = 0; i < parse_triple_num; i++)
		{
			this->triples_num++;

			//if the _id_tuples exceeds, double the space
			if (_id_tuples_size == _id_tuples_max)
			{
				int _new_tuples_len = _id_tuples_max * 2;
				int** _new_id_tuples = new int*[_new_tuples_len];
				memcpy(_new_id_tuples, _p_id_tuples, sizeof(int*) * _id_tuples_max);
				delete[] _p_id_tuples;
				_p_id_tuples = _new_id_tuples;
				_id_tuples_max = _new_tuples_len;
			}

			// For subject
			// (all subject is entity, some object is entity, the other is literal)
			string _sub = triple_array[i].getSubject();
			int _sub_id = (this->kvstore)->getIDByEntity(_sub);
			if (_sub_id == -1)
			{
				//_sub_id = this->entity_num;
				_sub_id = this->allocEntityID();
				//this->entity_num++;
				(this->kvstore)->setIDByEntity(_sub, _sub_id);
				(this->kvstore)->setEntityByID(_sub_id, _sub);
			}
			//  For predicate
			string _pre = triple_array[i].getPredicate();
			int _pre_id = (this->kvstore)->getIDByPredicate(_pre);
			if (_pre_id == -1)
			{
				//_pre_id = this->pre_num;
				_pre_id = this->allocPredicateID();
				//this->pre_num++;
				(this->kvstore)->setIDByPredicate(_pre, _pre_id);
				(this->kvstore)->setPredicateByID(_pre_id, _pre);
			}

			//  For object
			string _obj = triple_array[i].getObject();
			int _obj_id = -1;
			// obj is entity
			if (triple_array[i].isObjEntity())
			{
				_obj_id = (this->kvstore)->getIDByEntity(_obj);
				if (_obj_id == -1)
				{
					//_obj_id = this->entity_num;
					_obj_id = this->allocEntityID();
					//this->entity_num++;
					(this->kvstore)->setIDByEntity(_obj, _obj_id);
					(this->kvstore)->setEntityByID(_obj_id, _obj);
				}
			}
			//obj is literal
			if (triple_array[i].isObjLiteral())
			{
				_obj_id = (this->kvstore)->getIDByLiteral(_obj);
				if (_obj_id == -1)
				{
					//_obj_id = Util::LITERAL_FIRST_ID + (this->literal_num);
					_obj_id = this->allocLiteralID();
					//this->literal_num++;
					(this->kvstore)->setIDByLiteral(_obj, _obj_id);
					(this->kvstore)->setLiteralByID(_obj_id, _obj);
					//#ifdef DEBUG
					//if(_obj == "\"Bob\"")
					//{
					//cout << "this is id for Bob: " << _obj_id << endl;
					//}
					//cout<<"literal should be bob: " << kvstore->getLiteralByID(_obj_id)<<endl;
					//cout<<"id for bob: "<<kvstore->getIDByLiteral("\"Bob\"")<<endl;
					//#endif
				}
			}

			//  For id_tuples
			_p_id_tuples[_id_tuples_size] = new int[3];
			_p_id_tuples[_id_tuples_size][0] = _sub_id;
			_p_id_tuples[_id_tuples_size][1] = _pre_id;
			_p_id_tuples[_id_tuples_size][2] = _obj_id;
			_id_tuples_size++;

#ifdef DEBUG_PRECISE
			//  save six tuples
			{
				_six_tuples_fout << _sub_id << '\t'
					<< _pre_id << '\t'
					<< _obj_id << '\t'
					<< _sub << '\t'
					<< _pre << '\t'
					<< _obj << endl;
			}
#endif
			//_entity_bitset is used up, double the space
			if (this->entity_num >= entitybitset_max)
			{
				//cout<<"to double entity bitset num"<<endl;
				EntityBitSet** _new_entity_bitset = new EntityBitSet*[entitybitset_max * 2];
				memcpy(_new_entity_bitset, _entity_bitset, sizeof(EntityBitSet*) * entitybitset_max);
				delete[] _entity_bitset;
				_entity_bitset = _new_entity_bitset;

				int tmp = entitybitset_max * 2;
				for (int i = entitybitset_max; i < tmp; i++)
				{
					_entity_bitset[i] = new EntityBitSet();
					_entity_bitset[i]->reset();
				}

				entitybitset_max = tmp;
			}

			{
				_tmp_bitset.reset();
				Signature::encodePredicate2Entity(_pre_id, _tmp_bitset, Util::EDGE_OUT);
				Signature::encodeStr2Entity(_obj.c_str(), _tmp_bitset);
				*_entity_bitset[_sub_id] |= _tmp_bitset;
			}

			if (triple_array[i].isObjEntity())
			{
				_tmp_bitset.reset();
				Signature::encodePredicate2Entity(_pre_id, _tmp_bitset, Util::EDGE_IN);
				Signature::encodeStr2Entity(_sub.c_str(), _tmp_bitset);
				//cout<<"objid: "<<_obj_id <<endl;
				//when 15999 error
				//WARN:id allocated can be very large while the num is not so much
				//This means that maybe the space for the _obj_id is not allocated now
				//NOTICE:we prepare the free id list in uporder and contiguous, so in build process
				//this can work well
				*_entity_bitset[_obj_id] |= _tmp_bitset;
			}
		}
	}

	Util::logging("==> end while(true)");

	delete[] triple_array;
	_fin.close();
	_six_tuples_fout.close();


	{
		//save all entity_signature into binary file
		string _sig_binary_file = this->getSignatureBFile();
		FILE* _sig_fp = fopen(_sig_binary_file.c_str(), "wb");
		if (_sig_fp == NULL) {
			cerr << "Failed to open : " << _sig_binary_file << endl;
		}

		//EntityBitSet _all_bitset;
		for (int i = 0; i < this->entity_num; i++)
		{
			SigEntry* _sig = new SigEntry(EntitySig(*_entity_bitset[i]), i);
			fwrite(_sig, sizeof(SigEntry), 1, _sig_fp);
			//_all_bitset |= *_entity_bitset[i];
			delete _sig;
		}
		fclose(_sig_fp);

		for (int i = 0; i < entitybitset_max; i++)
		{
			delete _entity_bitset[i];
		}
		delete[] _entity_bitset;
	}

	{
		stringstream _ss;
		_ss << "finish sub2id pre2id obj2id" << endl;
		_ss << "tripleNum is " << this->triples_num << endl;
		_ss << "entityNum is " << this->entity_num << endl;
		_ss << "preNum is " << this->pre_num << endl;
		_ss << "literalNum is " << this->literal_num << endl;
		Util::logging(_ss.str());
		cout << _ss.str() << endl;
	}

	return true;
}

//NOTICE: below are the the new ones
bool
Database::s2p_s2o_s2po_sp2o(int** _p_id_tuples, int _id_tuples_max)
//Database::s2p_s2o_s2po_sp2o_sp2n(int** _p_id_tuples, int _id_tuples_max)
{
	qsort(_p_id_tuples, this->triples_num, sizeof(int*), Database::_spo_cmp);

	int* _oidlist_s = NULL;
	int* _pidlist_s = NULL;
	int* _oidlist_sp = NULL;
	int* _pidoidlist_s = NULL;

	int _oidlist_s_len = 0;
	int _pidlist_s_len = 0;
	int _oidlist_sp_len = 0;
	int _pidoidlist_s_len = 0;

	// only _oidlist_s will be assigned with space, _oidlist_sp is always a part of _oidlist_s, just a pointer is enough
	int _pidlist_max = 0;
	int _pidoidlist_max = 0;
	int _oidlist_max = 0;

	//true means next sub is a different one from the previous one
	bool _sub_change = true;
	//true means next <sub,pre> is different from the previous pair
	bool _sub_pre_change = true;
	//true means next pre is different from the previous one
	bool _pre_change = true;

	//Util::logging("finish s2p_sp2o_s2po initial");
	cerr << "finish s2p_sp2o_s2po initial" << endl;

	(this->kvstore)->open_subID2objIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_subID2preIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_subIDpreID2objIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_subID2preIDobjIDlist(KVstore::CREATE_MODE);

	//(this->kvstore)->open_subIDpreID2num(KVstore::CREATE_MODE);

	for (int i = 0; i < this->triples_num; i++)
		if (i + 1 == this->triples_num || (_p_id_tuples[i][0] != _p_id_tuples[i + 1][0] || _p_id_tuples[i][1] != _p_id_tuples[i + 1][1] || _p_id_tuples[i][2] != _p_id_tuples[i + 1][2]))
		{
			//cout<<"this is the "<<i<<"th loop"<<endl;
			if (_sub_change)
			{
				//cout<<"subject changed!"<<endl;
				//
				//QUERY:pidlist and oidlist maybe different size, not always use 1000 as start?
				//
				//pidlist
				_pidlist_max = 1000;
				_pidlist_s = new int[_pidlist_max];
				_pidlist_s_len = 0;
				//pidoidlist
				//_pidoidlist_max = 1000 * 2;
				//_pidoidlist_s = new int[_pidoidlist_max];
				//_pidoidlist_s_len = 0;

				//NOTICE:when debug, can change 1000 to 10 to discover problem
				//oidlist
				_oidlist_max = 1000;
				_oidlist_s = new int[_oidlist_max];
				_oidlist_sp = _oidlist_s;
				_oidlist_s_len = 0;
				_oidlist_sp_len = 0;
				/* pidoidlist */
				_pidoidlist_max = 1000 * 2;
				_pidoidlist_s = new int[_pidoidlist_max];
				_pidoidlist_s_len = 0;
			}
			//if(_sub_pre_change)
			//{
			////oidlist
			//_oidlist_max = 1000;
			//_oidlist_sp = new int[_oidlist_max];
			//_oidlist_sp_len = 0;
			//}

			//enlarge the space when needed
			if (_pidlist_s_len == _pidlist_max)
			{
				_pidlist_max *= 10;
				int* _new_pidlist_s = new int[_pidlist_max];
				memcpy(_new_pidlist_s, _pidlist_s, sizeof(int) * _pidlist_s_len);
				delete[] _pidlist_s;
				_pidlist_s = _new_pidlist_s;
			}

			//enlarge the space when needed
			if (_oidlist_s_len == _oidlist_max)
			{
				//cout<<"s2o need to enlarge"<<endl;
				_oidlist_max *= 10;
				int * _new_oidlist_s = new int[_oidlist_max];
				memcpy(_new_oidlist_s, _oidlist_s, sizeof(int) * _oidlist_s_len);
				/* (_oidlist_sp-_oidlist_s) is the offset of _oidlist_sp */
				_oidlist_sp = _new_oidlist_s + (_oidlist_sp - _oidlist_s);
				delete[] _oidlist_s;
				_oidlist_s = _new_oidlist_s;
			}
			//enalrge the space when needed
			//if(_oidlist_sp_len == _oidlist_max)
			//{
			//	_oidlist_max *= 10;
			//	int* _new_oidlist_sp = new int[_oidlist_max];
			//	memcpy(_new_oidlist_sp, _oidlist_sp, sizeof(int) * _oidlist_sp_len);
			//	delete[] _oidlist_sp;
			//	_oidlist_sp = _new_oidlist_sp;
			//}

			//enlarge the space when needed
			if (_pidoidlist_s_len == _pidoidlist_max)
			{
				_pidoidlist_max *= 10;
				int* _new_pidoidlist_s = new int[_pidoidlist_max];
				memcpy(_new_pidoidlist_s, _pidoidlist_s, sizeof(int) * _pidoidlist_s_len);
				delete[] _pidoidlist_s;
				_pidoidlist_s = _new_pidoidlist_s;
			}

			int _sub_id = _p_id_tuples[i][0];
			int _pre_id = _p_id_tuples[i][1];
			int _obj_id = _p_id_tuples[i][2];
			//		{
			//			stringstream _ss;
			//			_ss << _sub_id << "\t" << _pre_id << "\t" << _obj_id << endl;
			//			Util::logging(_ss.str());
			//		}

			_oidlist_s[_oidlist_s_len] = _obj_id;
			if (_sub_pre_change)
			{
				_oidlist_sp = _oidlist_s + _oidlist_s_len;
			}
			_oidlist_s_len++;
			_oidlist_sp_len++;

			//add objid to list
			//_oidlist_sp[_oidlist_sp_len++] = _obj_id;
			//WARN:this is wrong!

			//add <preid, objid> to list
			_pidoidlist_s[_pidoidlist_s_len] = _pre_id;
			_pidoidlist_s[_pidoidlist_s_len + 1] = _obj_id;
			_pidoidlist_s_len += 2;

			//add preid to list
			_pidlist_s[_pidlist_s_len++] = _pre_id;
			//NOTICE:this should not be placed in sub_pre_change clause
			//because it may cause error when removing

			//whether sub in new triple changes or not
			_sub_change = (i + 1 == this->triples_num) ||
				(_p_id_tuples[i][0] != _p_id_tuples[i + 1][0]);

			//whether pre in new triple changes or not
			_pre_change = (i + 1 == this->triples_num) ||
				(_p_id_tuples[i][1] != _p_id_tuples[i + 1][1]);

			//whether <sub,pre> in new triple changes or not
			_sub_pre_change = _sub_change || _pre_change;

			if (_sub_pre_change)
			{
				//map subid&preid 2 triple num
				//(this->kvstore)->setNumBysubIDpreID(_sub_id, _pre_id, _oidlist_sp_len);
				//
				//map subid&preid 2 objidlist
				(this->kvstore)->addobjIDlistBysubIDpreID(_sub_id, _pre_id, _oidlist_sp, _oidlist_sp_len);
				//if not use s2o memory
				//delete[] _oidlist_sp;
				_oidlist_sp = NULL;
				_oidlist_sp_len = 0;
			}

			if (_sub_change)
			{
				(this->kvstore)->addpreIDlistBysubID(_sub_id, _pidlist_s, _pidlist_s_len);
				delete[] _pidlist_s;
				_pidlist_s = NULL;
				_pidlist_s_len = 0;
				//map subid 2 preid&objidlist
				(this->kvstore)->addpreIDobjIDlistBysubID(_sub_id, _pidoidlist_s, _pidoidlist_s_len);
				delete[] _pidoidlist_s;
				_pidoidlist_s = NULL;
				_pidoidlist_s_len = 0;

				Util::sort(_oidlist_s, _oidlist_s_len);
				//NOTICE+WARN:not ok to remove duplicates here
				//s p1 o; s p2 o   the case is rare
				//and if we remove duplicates, we can only see one in s2o
				//This can cause error when remove triples, because we can not know if we should remove the oid
				//And we can not know if the node is isolate when removing a triple
				//_oidlist_s_len = Util::removeDuplicate(_oidlist_s, _oidlist_s_len);
				(this->kvstore)->addobjIDlistBysubID(_sub_id, _oidlist_s, _oidlist_s_len);
				delete[] _oidlist_s;
				_oidlist_sp = _oidlist_s = NULL;
				_oidlist_sp_len = _oidlist_s_len = 0;
			}

		}//end for( 0 to this->triple_num)

		 //Util::logging("OUT s2po...");
	cerr << "OUT s2po..." << endl;

	(this->kvstore)->close_subID2objIDlist();
	(this->kvstore)->close_subID2preIDlist();
	(this->kvstore)->close_subIDpreID2objIDlist();
	(this->kvstore)->close_subID2preIDobjIDlist();

	return true;
}

bool
Database::o2p_o2s_o2ps_op2s(int** _p_id_tuples, int _id_tuples_max)
//Database::o2p_o2s_o2ps_op2s_op2n(int** _p_id_tuples, int _id_tuples_max)
{
	//Util::logging("IN o2ps...");
	cerr << "IN o2ps..." << endl;

	qsort(_p_id_tuples, this->triples_num, sizeof(int**), Database::_ops_cmp);
	int* _sidlist_o = NULL;
	int* _sidlist_op = NULL;
	int* _pidsidlist_o = NULL;
	int* _pidlist_o = NULL;
	int _sidlist_o_len = 0;
	int _sidlist_op_len = 0;
	int _pidsidlist_o_len = 0;
	int _pidlist_o_len = 0;

	//only _sidlist_o will be assigned with space _sidlist_op is always a part of _sidlist_o just a pointer is enough
	int _sidlist_max = 0;
	int _pidsidlist_max = 0;
	int _pidlist_max = 0;

	//true means next obj is a different one from the previous one
	bool _obj_change = true;

	//true means next pre is a different one from the previous one
	bool _pre_change = true;

	//true means next <obj,pre> is different from the previous pair
	bool _obj_pre_change = true;

	(this->kvstore)->open_objID2subIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_objIDpreID2subIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_objID2preIDsubIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_objID2preIDlist(KVstore::CREATE_MODE);

	//(this->kvstore)->open_objIDpreID2num(KVstore::CREATE_MODE);

	for (int i = 0; i < this->triples_num; i++)
		if (i + 1 == this->triples_num || (_p_id_tuples[i][0] != _p_id_tuples[i + 1][0] || _p_id_tuples[i][1] != _p_id_tuples[i + 1][1] || _p_id_tuples[i][2] != _p_id_tuples[i + 1][2]))
		{
			if (_obj_change)
			{
				//sidlist
				_sidlist_max = 1000;
				_sidlist_o = new int[_sidlist_max];
				_sidlist_op = _sidlist_o;
				_sidlist_o_len = 0;
				_sidlist_op_len = 0;
				//pidsidlist
				_pidsidlist_max = 1000 * 2;
				_pidsidlist_o = new int[_pidsidlist_max];
				_pidsidlist_o_len = 0;
				//pidlist
				_pidlist_max = 1000;
				_pidlist_o = new int[_pidlist_max];
				_pidlist_o_len = 0;
			}

			//enlarge the space when needed
			if (_sidlist_o_len == _sidlist_max)
			{
				_sidlist_max *= 10;
				int * _new_sidlist_o = new int[_sidlist_max];
				memcpy(_new_sidlist_o, _sidlist_o, sizeof(int)*_sidlist_o_len);
				/* (_sidlist_op-_sidlist_o) is the offset of _sidlist_op */
				_sidlist_op = _new_sidlist_o + (_sidlist_op - _sidlist_o);
				delete[] _sidlist_o;
				_sidlist_o = _new_sidlist_o;
			}

			//enlarge the space when needed
			if (_pidsidlist_o_len == _pidsidlist_max)
			{
				_pidsidlist_max *= 10;
				int* _new_pidsidlist_o = new int[_pidsidlist_max];
				memcpy(_new_pidsidlist_o, _pidsidlist_o, sizeof(int) * _pidsidlist_o_len);
				delete[] _pidsidlist_o;
				_pidsidlist_o = _new_pidsidlist_o;
			}

			//enlarge the space when needed
			if (_pidlist_o_len == _pidlist_max)
			{
				_pidlist_max *= 10;
				int* _new_pidlist_o = new int[_pidlist_max];
				memcpy(_new_pidlist_o, _pidlist_o, sizeof(int) * _pidlist_o_len);
				delete[] _pidlist_o;
				_pidlist_o = _new_pidlist_o;
			}

			int _sub_id = _p_id_tuples[i][0];
			int _pre_id = _p_id_tuples[i][1];
			int _obj_id = _p_id_tuples[i][2];

			//add subid to list
			_sidlist_o[_sidlist_o_len] = _sub_id;

			//if <objid, preid> changes, _sidlist_op should be adjusted
			if (_obj_pre_change)
			{
				_sidlist_op = _sidlist_o + _sidlist_o_len;
			}

			_sidlist_o_len++;
			_sidlist_op_len++;

			//add <preid, subid> to list
			_pidsidlist_o[_pidsidlist_o_len] = _pre_id;
			_pidsidlist_o[_pidsidlist_o_len + 1] = _sub_id;;
			_pidsidlist_o_len += 2;

			//add preid to list
			_pidlist_o[_pidlist_o_len++] = _pre_id;
			//NOTICE:this should not be placed in sub_pre_change clause
			//because it may cause error when removing

			//whether sub in new triple changes or not
			_obj_change = (i + 1 == this->triples_num) ||
				(_p_id_tuples[i][2] != _p_id_tuples[i + 1][2]);

			//whether pre in new triple changes or not
			_pre_change = (i + 1 == this->triples_num) ||
				(_p_id_tuples[i][1] != _p_id_tuples[i + 1][1]);

			//whether <sub,pre> in new triple changes or not
			_obj_pre_change = _obj_change || _pre_change;

			if (_obj_pre_change)
			{
				//map objid&preid 2 triple num
				//(this->kvstore)->setNumByobjIDpreID(_obj_id, _pre_id, _sidlist_op_len);
				//
				//map objid&preid 2 subidlist
				(this->kvstore)->addsubIDlistByobjIDpreID(_obj_id, _pre_id, _sidlist_op, _sidlist_op_len);
				_sidlist_op = NULL;
				_sidlist_op_len = 0;
			}

			if (_obj_change)
			{
				//map objid 2 subidlist
				Util::sort(_sidlist_o, _sidlist_o_len);
				//NOTICE+WARN:the same reason as in s2o index
				//_sidlist_o_len = Util::removeDuplicate(_sidlist_o, _sidlist_o_len);
				(this->kvstore)->addsubIDlistByobjID(_obj_id, _sidlist_o, _sidlist_o_len);
				delete[] _sidlist_o;
				_sidlist_o = NULL;
				_sidlist_op = NULL;
				_sidlist_o_len = 0;

				//map objid 2 preid&subidlist
				(this->kvstore)->addpreIDsubIDlistByobjID(_obj_id, _pidsidlist_o, _pidsidlist_o_len);
				delete[] _pidsidlist_o;
				_pidsidlist_o = NULL;
				_pidsidlist_o_len = 0;

				//map objid 2 preidlist
				(this->kvstore)->addpreIDlistByobjID(_obj_id, _pidlist_o, _pidlist_o_len);
				delete[] _pidlist_o;
				_pidlist_o = NULL;
				_pidlist_o_len = 0;
			}

		}//end for( 0 to this->triple_num)


		 //Util::logging("OUT o2ps...");
	cerr << "OUT o2ps..." << endl;

	(this->kvstore)->close_objID2subIDlist();
	(this->kvstore)->close_objIDpreID2subIDlist();
	(this->kvstore)->close_objID2preIDsubIDlist();
	(this->kvstore)->close_objID2preIDlist();

	return true;
}

bool
Database::p2s_p2o_p2so(int** _p_id_tuples, int _id_tuples_max)
//Database::p2s_p2o_p2so_p2n(int** _p_id_tuples, int _id_tuples_max)
{
	qsort(_p_id_tuples, this->triples_num, sizeof(int*), Database::_pso_cmp);
	int* _sidlist_p = NULL;
	int* _oidlist_p = NULL;
	int* _sidoidlist_p = NULL;
	int _sidlist_p_len = 0;
	int _oidlist_p_len = 0;
	int _sidoidlist_p_len = 0;
	int _sidlist_max = 0;
	int _sidoidlist_max = 0;
	int _oidlist_max = 0;

	//true means next pre is different from the previous one
	bool _pre_change = true;
	bool _sub_change = true;
	//bool _pre_sub_change = true;

	//Util::logging("finish p2s_p2o_p2so initial");
	cerr << "finish p2s_p2o_p2so initial" << endl;

	(this->kvstore)->open_preID2subIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_preID2objIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_preID2subIDobjIDlist(KVstore::CREATE_MODE);

	//(this->kvstore)->open_preID2num(KVstore::CREATE_MODE);

	for (int i = 0; i < this->triples_num; i++)
		if (i + 1 == this->triples_num || (_p_id_tuples[i][0] != _p_id_tuples[i + 1][0] || _p_id_tuples[i][1] != _p_id_tuples[i + 1][1] || _p_id_tuples[i][2] != _p_id_tuples[i + 1][2]))
		{
			if (_pre_change)
			{
				//sidlist
				_sidlist_max = 1000;
				_sidlist_p = new int[_sidlist_max];
				_sidlist_p_len = 0;
				//sidoidlist
				_sidoidlist_max = 1000 * 2;
				_sidoidlist_p = new int[_sidoidlist_max];
				_sidoidlist_p_len = 0;
				//oidlist
				_oidlist_max = 1000;
				_oidlist_p = new int[_oidlist_max];
				_oidlist_p_len = 0;
			}

			//enlarge the space when needed
			if (_sidlist_p_len == _sidlist_max)
			{
				_sidlist_max *= 10;
				int* _new_sidlist_p = new int[_sidlist_max];
				memcpy(_new_sidlist_p, _sidlist_p, sizeof(int) * _sidlist_p_len);
				delete[] _sidlist_p;
				_sidlist_p = _new_sidlist_p;
			}

			//enalrge the space when needed
			if (_oidlist_p_len == _oidlist_max)
			{
				_oidlist_max *= 10;
				int* _new_oidlist_p = new int[_oidlist_max];
				memcpy(_new_oidlist_p, _oidlist_p, sizeof(int) * _oidlist_p_len);
				delete[] _oidlist_p;
				_oidlist_p = _new_oidlist_p;
			}

			//enlarge the space when needed
			if (_sidoidlist_p_len == _sidoidlist_max)
			{
				_sidoidlist_max *= 10;
				int* _new_sidoidlist_p = new int[_sidoidlist_max];
				memcpy(_new_sidoidlist_p, _sidoidlist_p, sizeof(int) * _sidoidlist_p_len);
				delete[] _sidoidlist_p;
				_sidoidlist_p = _new_sidoidlist_p;
			}

			int _sub_id = _p_id_tuples[i][0];
			int _pre_id = _p_id_tuples[i][1];
			int _obj_id = _p_id_tuples[i][2];
			//		{
			//			stringstream _ss;
			//			_ss << _sub_id << "\t" << _pre_id << "\t" << _obj_id << endl;
			//			Util::logging(_ss.str());
			//		}

			//add objid to list
			_oidlist_p[_oidlist_p_len++] = _obj_id;

			//add <subid, objid> to list
			_sidoidlist_p[_sidoidlist_p_len] = _sub_id;
			_sidoidlist_p[_sidoidlist_p_len + 1] = _obj_id;
			_sidoidlist_p_len += 2;

			//whether sub in new triple changes or not
			_pre_change = (i + 1 == this->triples_num) || (_p_id_tuples[i][1] != _p_id_tuples[i + 1][1]);
			_sub_change = (i + 1 == this->triples_num) || (_p_id_tuples[i][0] != _p_id_tuples[i + 1][0]);
			//_pre_sub_change = _pre_change || _sub_change;

			//if(_pre_sub_change)
			//{
			//add subid to list
			_sidlist_p[_sidlist_p_len++] = _sub_id;
			//}

			if (_pre_change)
			{
				//map preid 2 subidlist
				(this->kvstore)->addsubIDlistBypreID(_pre_id, _sidlist_p, _sidlist_p_len);
				delete[] _sidlist_p;
				_sidlist_p = NULL;
				_sidlist_p_len = 0;
				//map preid 2 objidlist
				Util::sort(_oidlist_p, _oidlist_p_len);
				//_oidlist_p_len = Util::removeDuplicate(_oidlist_p, _oidlist_p_len);
				(this->kvstore)->addobjIDlistBypreID(_pre_id, _oidlist_p, _oidlist_p_len);
				delete[] _oidlist_p;
				_oidlist_p = NULL;
				_oidlist_p_len = 0;
				//map preid 2 triple num
				//(this->kvstore)->setNumBypreID(_pre_id, _sidoidlist_p_len/2);
				//
				//map preid 2 subid&objidlist
				(this->kvstore)->addsubIDobjIDlistBypreID(_pre_id, _sidoidlist_p, _sidoidlist_p_len);
				delete[] _sidoidlist_p;
				_sidoidlist_p = NULL;
				_sidoidlist_p_len = 0;
			}

		}//end for( 0 to this->triple_num)

		 //Util::logging("OUT p2so...");
	cerr << "OUT p2so..." << endl;

	(this->kvstore)->close_preID2subIDlist();
	(this->kvstore)->close_preID2objIDlist();
	(this->kvstore)->close_preID2subIDobjIDlist();

	return true;
}

bool
Database::so2p_s2o(int** _p_id_tuples, int _id_tuples_max)
{
	qsort(_p_id_tuples, this->triples_num, sizeof(int*), Database::_sop_cmp);
	int* _pidlist_so = NULL;
	int* _oidlist_s = NULL;
	int _pidlist_so_len = 0;
	int _oidlist_s_len = 0;
	int _pidlist_max = 0;
	int _oidlist_max = 0;

	//true means next sub is a different one from the previous one
	bool _sub_change = true;
	//true means next <sub,obj> is different from the previous pair
	bool _sub_obj_change = true;
	//true means next pre is different from the previous one
	bool _obj_change = true;

	bool _pre_change = true;
	bool _sub_obj_pre_change = true;

	Util::logging("finish so2p_s2o initial");

	(this->kvstore)->open_subIDobjID2preIDlist(KVstore::CREATE_MODE);
	(this->kvstore)->open_subID2objIDlist(KVstore::CREATE_MODE);

	for (int i = 0; i < this->triples_num; i++)
		if (i + 1 == this->triples_num || (_p_id_tuples[i][0] != _p_id_tuples[i + 1][0] || _p_id_tuples[i][1] != _p_id_tuples[i + 1][1] || _p_id_tuples[i][2] != _p_id_tuples[i + 1][2]))
		{
			if (_sub_change)
			{
				//oidlist
				_oidlist_max = 1000 * 2;
				_oidlist_s = new int[_oidlist_max];
				_oidlist_s_len = 0;
			}
			if (_sub_obj_change)
			{
				//pidlist
				_pidlist_max = 1000;
				_pidlist_so = new int[_pidlist_max];
				_pidlist_so_len = 0;
			}

			//enlarge the space when needed
			if (_pidlist_so_len == _pidlist_max)
			{
				_pidlist_max *= 10;
				int* _new_pidlist_so = new int[_pidlist_max];
				memcpy(_new_pidlist_so, _pidlist_so, sizeof(int) * _pidlist_so_len);
				delete[] _pidlist_so;
				_pidlist_so = _new_pidlist_so;
			}

			//enalrge the space when needed
			if (_oidlist_s_len == _oidlist_max)
			{
				_oidlist_max *= 10;
				int* _new_oidlist_s = new int[_oidlist_max];
				memcpy(_new_oidlist_s, _oidlist_s, sizeof(int) * _oidlist_s_len);
				delete[] _oidlist_s;
				_oidlist_s = _new_oidlist_s;
			}

			int _sub_id = _p_id_tuples[i][0];
			int _pre_id = _p_id_tuples[i][1];
			int _obj_id = _p_id_tuples[i][2];
#ifdef DEBUG
			//cout << kvstore->getEntityByID(_sub_id) << endl;
			//cout << kvstore->getPredicateByID(_pre_id) << endl;
			//if(_obj_id < Util::LITERAL_FIRST_ID)
			//cout<<kvstore->getEntityByID(_obj_id)<<endl;
			//else
			//cout<<kvstore->getLiteralByID(_obj_id)<<endl;
#endif

			//whether sub in new triple changes or not
			_sub_change = (i + 1 == this->triples_num) ||
				(_p_id_tuples[i][0] != _p_id_tuples[i + 1][0]);

			//whether pre in new triple changes or not
			_obj_change = (i + 1 == this->triples_num) ||
				(_p_id_tuples[i][2] != _p_id_tuples[i + 1][2]);

			_pre_change = (i + 1 == this->triples_num) ||
				(_p_id_tuples[i][1] != _p_id_tuples[i + 1][1]);

			//whether <sub,pre> in new triple changes or not
			_sub_obj_change = _sub_change || _obj_change;

			_sub_obj_pre_change = _sub_obj_change || _pre_change;

			if (_sub_obj_pre_change)
			{
				//cout << "add pre to so2p" << endl;
				//add preid to list
				_pidlist_so[_pidlist_so_len++] = _pre_id;
			}

			if (_sub_obj_change)
			{
				//cout<<"add obj to s2o" << endl;
				//add objid to list
				_oidlist_s[_oidlist_s_len++] = _obj_id;

				//cout<<"set so2p list"<<endl;
				//cout<<"objid: "<<_obj_id<<endl;
				(this->kvstore)->addpreIDlistBysubIDobjID(_sub_id, _obj_id, _pidlist_so, _pidlist_so_len);
				delete[] _pidlist_so;
				_pidlist_so = NULL;
				_pidlist_so_len = 0;
			}

			if (_sub_change)
			{
				//cout<<"set s2o list"<<endl;
				(this->kvstore)->addobjIDlistBysubID(_sub_id, _oidlist_s, _oidlist_s_len);
				delete[] _oidlist_s;
				_oidlist_s = NULL;
				_oidlist_s_len = 0;
			}
		}//end for( 0 to this->triple_num)
		 //int* list = NULL;
		 //int len = 0;
		 //kvstore->getpreIDlistBysubIDobjID(4, 1000000004, list, len);
		 //for(int i = 0; i < len; ++i)
		 //{
		 //cout << "predicate "  << list[i] << endl;
		 //cout << kvstore->getPredicateByID(list[i]) << endl;
		 //}
		 //cout << "the end" << endl;

	Util::logging("OUT so2p...");

	//BETTER:to release the trees here to save memory

	return true;
}

//compare function for qsort
int
Database::_spo_cmp(const void* _a, const void* _b)
{
	int** _p_a = (int**)_a;
	int** _p_b = (int**)_b;

	{   /* compare subid first */
		int _sub_id_a = (*_p_a)[0];
		int _sub_id_b = (*_p_b)[0];
		if (_sub_id_a != _sub_id_b)
		{
			return _sub_id_a - _sub_id_b;
		}
	}

	{   /* then preid */
		int _pre_id_a = (*_p_a)[1];
		int _pre_id_b = (*_p_b)[1];
		if (_pre_id_a != _pre_id_b)
		{
			return _pre_id_a - _pre_id_b;
		}
	}

	{   /* objid at last */
		int _obj_id_a = (*_p_a)[2];
		int _obj_id_b = (*_p_b)[2];
		if (_obj_id_a != _obj_id_b)
		{
			return _obj_id_a - _obj_id_b;
		}
	}
	return 0;
}

int
Database::_ops_cmp(const void* _a, const void* _b)
{
	int** _p_a = (int**)_a;
	int** _p_b = (int**)_b;
	{   /* compare objid first */
		int _obj_id_a = (*_p_a)[2];
		int _obj_id_b = (*_p_b)[2];
		if (_obj_id_a != _obj_id_b)
		{
			return _obj_id_a - _obj_id_b;
		}
	}

	{   /* then preid */
		int _pre_id_a = (*_p_a)[1];
		int _pre_id_b = (*_p_b)[1];
		if (_pre_id_a != _pre_id_b)
		{
			return _pre_id_a - _pre_id_b;
		}
	}

	{   /* subid at last */
		int _sub_id_a = (*_p_a)[0];
		int _sub_id_b = (*_p_b)[0];
		if (_sub_id_a != _sub_id_b)
		{
			return _sub_id_a - _sub_id_b;
		}
	}

	return 0;
}

int
Database::_pso_cmp(const void* _a, const void* _b)
{
	int** _p_a = (int**)_a;
	int** _p_b = (int**)_b;
	{   /* compare preid first */
		int _pre_id_a = (*_p_a)[1];
		int _pre_id_b = (*_p_b)[1];
		if (_pre_id_a != _pre_id_b)
		{
			return _pre_id_a - _pre_id_b;
		}
	}

	{   /* then subid */
		int _sub_id_a = (*_p_a)[0];
		int _sub_id_b = (*_p_b)[0];
		if (_sub_id_a != _sub_id_b)
		{
			return _sub_id_a - _sub_id_b;
		}
	}

	{   /* objid at last */
		int _obj_id_a = (*_p_a)[2];
		int _obj_id_b = (*_p_b)[2];
		if (_obj_id_a != _obj_id_b)
		{
			return _obj_id_a - _obj_id_b;
		}
	}

	return 0;
}

int
Database::_sop_cmp(const void* _a, const void* _b)
{
	int** _p_a = (int**)_a;
	int** _p_b = (int**)_b;
	{   /* compare subid first */
		int _sub_id_a = (*_p_a)[0];
		int _sub_id_b = (*_p_b)[0];
		if (_sub_id_a != _sub_id_b)
		{
			return _sub_id_a - _sub_id_b;
		}
	}

	{   /* then objid */
		int _obj_id_a = (*_p_a)[2];
		int _obj_id_b = (*_p_b)[2];
		if (_obj_id_a != _obj_id_b)
		{
			return _obj_id_a - _obj_id_b;
		}
	}

	{   /* preid at last */
		int _pre_id_a = (*_p_a)[1];
		int _pre_id_b = (*_p_b)[1];
		if (_pre_id_a != _pre_id_b)
		{
			return _pre_id_a - _pre_id_b;
		}
	}

	return 0;
}

int
Database::insertTriple(const TripleWithObjType& _triple, vector<int>* _vertices, vector<int>* _predicates)
{
	//cout<<endl<<"the new triple is:"<<endl;
	//cout<<_triple.subject<<endl;
	//cout<<_triple.predicate<<endl;
	//cout<<_triple.object<<endl;

	//int sid1 = this->kvstore->getIDByEntity("<http://www.Department7.University0.edu/UndergraduateStudent394>");
	//int sid2 = this->kvstore->getIDByEntity("<http://www.Department7.University0.edu/UndergraduateStudent395>");
	//int oid1 = this->kvstore->getIDByEntity("<ub:UndergraduateStudent>");
	//int oid2 = this->kvstore->getIDByEntity("<UndergraduateStudent394@Department7.University0.edu>");
	//cout<<sid1<<" "<<sid2<<" "<<oid1<<" "<<oid2<<endl;

	long tv_kv_store_begin = Util::get_cur_time();

	int _sub_id = (this->kvstore)->getIDByEntity(_triple.subject);
	bool _is_new_sub = false;
	//if sub does not exist
	if (_sub_id == -1)
	{
		_is_new_sub = true;
		//_sub_id = this->entity_num++;
		_sub_id = this->allocEntityID();
		//cout<<"this is a new sub id"<<endl;
		//if(_sub_id == 14912)
		//{
			//cout<<"get the error one"<<endl;
			//cout<<_sub_id<<endl<<_triple.subject<<endl;
			//cout<<_triple.predicate<<endl<<_triple.object<<endl;
		//}
		this->sub_num++;
		(this->kvstore)->setIDByEntity(_triple.subject, _sub_id);
		(this->kvstore)->setEntityByID(_sub_id, _triple.subject);

		if(_vertices != NULL)
			_vertices->push_back(_sub_id);
	}

	int _pre_id = (this->kvstore)->getIDByPredicate(_triple.predicate);
	bool _is_new_pre = false;
	//if pre does not exist
	if (_pre_id == -1)
	{
		_is_new_pre = true;
		//_pre_id = this->pre_num++;
		_pre_id = this->allocPredicateID();
		(this->kvstore)->setIDByPredicate(_triple.predicate, _pre_id);
		(this->kvstore)->setPredicateByID(_pre_id, _triple.predicate);

		if(_predicates != NULL)
			_predicates->push_back(_pre_id);
	}

	//object is either entity or literal
	int _obj_id = -1;
	bool _is_new_obj = false;
	bool is_obj_entity = _triple.isObjEntity();
	if (is_obj_entity)
	{
		_obj_id = (this->kvstore)->getIDByEntity(_triple.object);

		if (_obj_id == -1)
		{
			_is_new_obj = true;
			//_obj_id = this->entity_num++;
			_obj_id = this->allocEntityID();
			(this->kvstore)->setIDByEntity(_triple.object, _obj_id);
			(this->kvstore)->setEntityByID(_obj_id, _triple.object);

			if(_vertices != NULL)
				_vertices->push_back(_obj_id);
		}
	}
	else
	{
		_obj_id = (this->kvstore)->getIDByLiteral(_triple.object);

		if (_obj_id == -1)
		{
			_is_new_obj = true;
			//_obj_id = Util::LITERAL_FIRST_ID + this->literal_num;
			_obj_id = this->allocLiteralID();
			(this->kvstore)->setIDByLiteral(_triple.object, _obj_id);
			(this->kvstore)->setLiteralByID(_obj_id, _triple.object);

			if(_vertices != NULL)
				_vertices->push_back(_obj_id);
		}
	}

	//if this is not a new triple, return directly
	bool _triple_exist = false;
	if (!_is_new_sub && !_is_new_pre && !_is_new_obj)
	{
		_triple_exist = this->exist_triple(_sub_id, _pre_id, _obj_id);
	}

	//debug
	//  {
	//      stringstream _ss;
	//      _ss << this->literal_num << endl;
	//      _ss <<"ids: " << _sub_id << " " << _pre_id << " " << _obj_id << " " << _triple_exist << endl;
	//      Util::logging(_ss.str());
	//  }

	if (_triple_exist)
	{
		cout<<"this triple already exist"<<endl;
		return 0;
	}
	else
	{
		this->triples_num++;
	}
	//cout<<"the triple spo ids: "<<_sub_id<<" "<<_pre_id<<" "<<_obj_id<<endl;

	//update sp2o op2s s2po o2ps s2o o2s etc.
	int updateLen = (this->kvstore)->updateTupleslist_insert(_sub_id, _pre_id, _obj_id);

	//int* list = NULL;
	//int len = 0;
	//int root = this->kvstore->getIDByEntity("<root>");
	//int contain = this->kvstore->getIDByPredicate("<contain>");
	//this->kvstore->getobjIDlistBysubIDpreID(root, contain, list, len);
	//cout<<root<<" "<<contain<<endl;
	//if(len == 0)
	//{
	//cout<<"no result"<<endl;
	//}
	//for(int i = 0; i < len; ++i)
	//{
	//cout << this->kvstore->getEntityByID(list[i])<<" "<<list[i]<<endl;
	//}

	long tv_kv_store_end = Util::get_cur_time();

	EntityBitSet _sub_entity_bitset;
	_sub_entity_bitset.reset();

	this->encodeTriple2SubEntityBitSet(_sub_entity_bitset, &_triple);

	//if new entity then insert it, else update it.
	if (_is_new_sub)
	{
		//cout<<"to insert: "<<_sub_id<<" "<<this->kvstore->getEntityByID(_sub_id)<<endl;
		SigEntry _sig(_sub_id, _sub_entity_bitset);
		(this->vstree)->insertEntry(_sig);
	}
	else
	{
		//cout<<"to update: "<<_sub_id<<" "<<this->kvstore->getEntityByID(_sub_id)<<endl;
		(this->vstree)->updateEntry(_sub_id, _sub_entity_bitset);
	}

	//if the object is an entity, then update or insert this entity's entry.
	if (is_obj_entity)
	{
		EntityBitSet _obj_entity_bitset;
		_obj_entity_bitset.reset();

		this->encodeTriple2ObjEntityBitSet(_obj_entity_bitset, &_triple);

		if (_is_new_obj)
		{
			//cout<<"to insert: "<<_obj_id<<" "<<this->kvstore->getEntityByID(_obj_id)<<endl;
			SigEntry _sig(_obj_id, _obj_entity_bitset);
			(this->vstree)->insertEntry(_sig);
		}
		else
		{
			//cout<<"to update: "<<_obj_id<<" "<<this->kvstore->getEntityByID(_obj_id)<<endl;
			(this->vstree)->updateEntry(_obj_id, _obj_entity_bitset);
		}
	}

	long tv_vs_store_end = Util::get_cur_time();

	//debug
	{
		cout << "update kv_store, used " << (tv_kv_store_end - tv_kv_store_begin) << "ms." << endl;
		cout << "update vs_store, used " << (tv_vs_store_end - tv_kv_store_end) << "ms." << endl;
	}

	return updateLen;
}

bool
Database::removeTriple(const TripleWithObjType& _triple, vector<int>* _vertices, vector<int>* _predicates)
{
	long tv_kv_store_begin = Util::get_cur_time();

	int _sub_id = (this->kvstore)->getIDByEntity(_triple.subject);
	int _pre_id = (this->kvstore)->getIDByPredicate(_triple.predicate);
	int _obj_id = (this->kvstore)->getIDByEntity(_triple.object);
	if (_obj_id == -1)
	{
		_obj_id = (this->kvstore)->getIDByLiteral(_triple.object);
	}

	if (_sub_id == -1 || _pre_id == -1 || _obj_id == -1)
	{
		return false;
	}
	bool _exist_triple = this->exist_triple(_sub_id, _pre_id, _obj_id);
	if (!_exist_triple)
	{
		return false;
	}
	else
	{
		this->triples_num--;
	}

	cout<<"triple existence checked"<<endl;

	//remove from sp2o op2s s2po o2ps s2o o2s
	//sub2id, pre2id and obj2id will not be updated
	(this->kvstore)->updateTupleslist_remove(_sub_id, _pre_id, _obj_id);
	cout<<"11 trees updated"<<endl;

	long tv_kv_store_end = Util::get_cur_time();

	int sub_degree = (this->kvstore)->getEntityDegree(_sub_id);
	//if subject become an isolated point, remove its corresponding entry
	if (sub_degree == 0)
	{
		//cout<<"to remove entry for sub"<<endl;
		//cout<<_sub_id << " "<<this->kvstore->getEntityByID(_sub_id)<<endl;
		this->kvstore->subEntityByID(_sub_id);
		this->kvstore->subIDByEntity(_triple.subject);
		(this->vstree)->removeEntry(_sub_id);
		this->freeEntityID(_sub_id);
		this->sub_num--;
	}
	//else re-calculate the signature of subject & replace that in vstree
	else
	{
		//cout<<"to replace entry for sub"<<endl;
		//cout<<_sub_id << " "<<this->kvstore->getEntityByID(_sub_id)<<endl;
		EntityBitSet _entity_bitset;
		_entity_bitset.reset();
		this->calculateEntityBitSet(_sub_id, _entity_bitset);
		//NOTICE:can not use updateEntry as insert because this is in remove
		//In insert we can add a OR operation and all is ok
		(this->vstree)->replaceEntry(_sub_id, _entity_bitset);
	}
	//cout<<"subject dealed"<<endl;

	bool is_obj_entity = _triple.isObjEntity();
	int obj_degree;
	if (is_obj_entity)
	{
		obj_degree = this->kvstore->getEntityDegree(_obj_id);
		if (obj_degree == 0)
		{
			//cout<<"to remove entry for obj"<<endl;
			//cout<<_obj_id << " "<<this->kvstore->getEntityByID(_obj_id)<<endl;
			this->kvstore->subEntityByID(_obj_id);
			this->kvstore->subIDByEntity(_triple.object);
			this->vstree->removeEntry(_obj_id);
			this->freeEntityID(_obj_id);
		}
		else
		{
			//cout<<"to replace entry for obj"<<endl;
			//cout<<_obj_id << " "<<this->kvstore->getEntityByID(_obj_id)<<endl;
			EntityBitSet _entity_bitset;
			_entity_bitset.reset();
			this->calculateEntityBitSet(_obj_id, _entity_bitset);
			this->vstree->replaceEntry(_obj_id, _entity_bitset);
		}
	}
	else
	{
		obj_degree = this->kvstore->getLiteralDegree(_obj_id);
		if (obj_degree == 0)
		{
			this->kvstore->subLiteralByID(_obj_id);
			this->kvstore->subIDByLiteral(_triple.object);
			this->freeLiteralID(_obj_id);
		}
	}
	//cout<<"object dealed"<<endl;

	int pre_degree = this->kvstore->getPredicateDegree(_pre_id);
	if (pre_degree == 0)
	{
		this->kvstore->subPredicateByID(_pre_id);
		this->kvstore->subIDByPredicate(_triple.predicate);
		this->freePredicateID(_pre_id);
	}
	//cout<<"predicate dealed"<<endl;

	long tv_vs_store_end = Util::get_cur_time();

	//debug
	{
		cout << "update kv_store, used " << (tv_kv_store_end - tv_kv_store_begin) << "ms." << endl;
		cout << "update vs_store, used " << (tv_vs_store_end - tv_kv_store_end) << "ms." << endl;
	}
	return true;
}

bool
Database::insert(std::string _rdf_file)
{
	bool flag = this->load();
	if (!flag)
	{
		return false;
	}
	cout << "finish loading" << endl;

	long tv_load = Util::get_cur_time();

	ifstream _fin(_rdf_file.c_str());
	if (!_fin)
	{
		cerr << "fail to open : " << _rdf_file << ".@insert_test" << endl;
		//exit(0);
		return false;
	}

	//NOTICE+WARN:we can not load all triples into memory all at once!!!
	//the parameter in build and insert must be the same, because RDF parser also use this
	//for build process, this one can be big enough if memory permits
	//for insert/delete process, this can not be too large, otherwise too costly
	TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];
	//parse a file
	RDFParser _parser(_fin);

	int triple_num = 0;
#ifdef DEBUG
	Util::logging("==> while(true)");
#endif
	while (true)
	{
		int parse_triple_num = 0;
		_parser.parseFile(triple_array, parse_triple_num);
#ifdef DEBUG
		stringstream _ss;
		//NOTICE:this is not same as others, use parse_triple_num directly
		_ss << "finish rdfparser" << parse_triple_num << endl;
		Util::logging(_ss.str());
		cout << _ss.str() << endl;
#endif
		if (parse_triple_num == 0)
		{
			break;
		}

		//Process the Triple one by one
		this->insert(triple_array, parse_triple_num);
		//some maybe invalid or duplicate
		//triple_num += parse_triple_num;
	}

	delete[] triple_array;
	long tv_insert = Util::get_cur_time();
	cout << "after insert, used " << (tv_insert - tv_load) << "ms." << endl;
	//BETTER:update kvstore and vstree separately, to lower the memory cost
	flag = this->vstree->saveTree();
	if (!flag)
	{
		return false;
	}
	flag = this->saveDBInfoFile();
	if (!flag)
	{
		return false;
	}

	cout << "insert rdf triples done." << endl;

	//int* list = NULL;
	//int len = 0;
	//int root = this->kvstore->getIDByEntity("<root>");
	//int contain = this->kvstore->getIDByPredicate("<contain>");
	//this->kvstore->getobjIDlistBysubIDpreID(root, contain, list, len);
	//cout<<root<<" "<<contain<<endl;
	//if(len == 0)
	//{
	//cout<<"no result"<<endl;
	//}
	//for(int i = 0; i < len; ++i)
	//{
	//cout << this->kvstore->getEntityByID(list[i])<<" "<<list[i]<<endl;
	//}
	//cout<<endl;
	//int t = this->kvstore->getIDByEntity("<node5>");
	//cout<<t<<endl;
	//cout<<this->kvstore->getEntityByID(0)<<endl;

	return true;
}

bool
Database::remove(std::string _rdf_file)
{
	bool flag = this->load();
	if (!flag)
	{
		return false;
	}
	cout << "finish loading" << endl;

	long tv_load = Util::get_cur_time();

	ifstream _fin(_rdf_file.c_str());
	if (!_fin)
	{
		cerr << "fail to open : " << _rdf_file << ".@remove_test" << endl;
		return false;
	}

	//NOTICE+WARN:we can not load all triples into memory all at once!!!
	TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];
	//parse a file
	RDFParser _parser(_fin);

	//int triple_num = 0;
#ifdef DEBUG
	Util::logging("==> while(true)");
#endif
	while (true)
	{
		int parse_triple_num = 0;
		_parser.parseFile(triple_array, parse_triple_num);
#ifdef DEBUG
		stringstream _ss;
		//NOTICE:this is not same as others, use parse_triple_num directly
		_ss << "finish rdfparser" << parse_triple_num << endl;
		Util::logging(_ss.str());
		cout << _ss.str() << endl;
#endif
		if (parse_triple_num == 0)
		{
			break;
		}

		this->remove(triple_array, parse_triple_num);
		//some maybe invalid or duplicate
		//triple_num -= parse_triple_num;
	}

	//TODO:better to free this just after id_tuples are ok
	//(only when using group insertion/deletion)
	//or reduce the array size
	delete[] triple_array;
	long tv_remove = Util::get_cur_time();
	cout << "after remove, used " << (tv_remove - tv_load) << "ms." << endl;

	flag = this->vstree->saveTree();
	if (!flag)
	{
		return false;
	}
	flag = this->saveDBInfoFile();
	if (!flag)
	{
		return false;
	}

	cout << "remove rdf triples done." << endl;
	return true;
}

bool
Database::insert(const TripleWithObjType* _triples, int _triple_num)
{
	vector<int> _vertices,  _predicates;

	//TODO:We do not consider vertices and predicates vectors now
	//nothing to do with deleteions because this will not affect the getFinalResult process(id2pos, pos2string)
	//(maybe need to set the id pos as invalid?)
	//when insertion, append the string and set the pos(search if id exist, then update or append the id2pos)
	
#ifdef USE_GROUP_INSERT
	//NOTICE:this is called by insert(file) or query()(but can not be too large),
	//assume that db is loaded already
	int** id_tuples = new int*[_triple_num];
	int valid_num = 0;
	int i = 0;
	//for(i = 0; i < _triple_num; ++i)
	//{
	//id_tuples[i] = new int[3];
	//}
	map<int, EntityBitSet> old_sigmap;
	map<int, EntityBitSet> new_sigmap;
	set<int> new_entity;
	map<int, EntityBitSet>::iterator it;
	EntityBitSet tmpset;
	tmpset.reset();

	int subid, objid, preid;
	bool is_obj_entity;
	for (i = 0; i < _triple_num; ++i)
	{
		bool is_new_sub = false, is_new_pre = false, is_new_obj = false;

		string sub = _triples[i].getSubject();
		subid = this->kvstore->getIDByEntity(sub);
		if (subid == -1)
		{
			is_new_sub = true;
			subid = this->allocEntityID();
			cout<<"this is a new subject: "<<sub<<" "<<subid<<endl;
			this->sub_num++;
			this->kvstore->setIDByEntity(sub, subid);
			this->kvstore->setEntityByID(subid, sub);
			new_entity.insert(subid);
		}

		string pre = _triples[i].getPredicate();
		preid = this->kvstore->getIDByPredicate(pre);
		if (preid == -1)
		{
			is_new_pre = true;
			preid = this->allocPredicateID();
			this->kvstore->setIDByPredicate(pre, preid);
			this->kvstore->setPredicateByID(preid, pre);
		}

		is_obj_entity = _triples[i].isObjEntity();
		string obj = _triples[i].getObject();
		if (is_obj_entity)
		{
			objid = this->kvstore->getIDByEntity(obj);
			if (objid == -1)
			{
				is_new_obj = true;
				objid = this->allocEntityID();
				cout<<"this is a new object: "<<obj<<" "<<objid<<endl;
				//this->obj_num++;
				this->kvstore->setIDByEntity(obj, objid);
				this->kvstore->setEntityByID(objid, obj);
				new_entity.insert(objid);
			}
		}
		else //isObjLiteral
		{
			objid = this->kvstore->getIDByLiteral(obj);
			if (objid == -1)
			{
				is_new_obj = true;
				objid = this->allocLiteralID();
				//this->obj_num++;
				this->kvstore->setIDByLiteral(obj, objid);
				this->kvstore->setLiteralByID(objid, obj);
			}
		}

		bool triple_exist = false;
		if (!is_new_sub && !is_new_pre && !is_new_obj)
		{
			triple_exist = this->exist_triple(subid, preid, objid);
		}
		if (triple_exist)
		{
			cout<<"this triple exist"<<endl;
			continue;
		}
		cout<<"this triple not exist"<<endl;

		id_tuples[valid_num] = new int[3];
		id_tuples[valid_num][0] = subid;
		id_tuples[valid_num][1] = preid;
		id_tuples[valid_num][2] = objid;
		this->triples_num++;
		valid_num++;

		tmpset.reset();
		Signature::encodePredicate2Entity(preid, tmpset, Util::EDGE_OUT);
		Signature::encodeStr2Entity(obj.c_str(), tmpset);
		if(new_entity.find(subid) != new_entity.end())
		{
			it = new_sigmap.find(subid);
			if (it != new_sigmap.end())
			{
				it->second |= tmpset;
			}
			else
			{
				new_sigmap[subid] = tmpset;
			}
		}
		else
		{
			it = old_sigmap.find(subid);
			if (it != old_sigmap.end())
			{
				it->second |= tmpset;
			}
			else
			{
				old_sigmap[subid] = tmpset;
			}
		}

		if (is_obj_entity)
		{
			tmpset.reset();
			Signature::encodePredicate2Entity(preid, tmpset, Util::EDGE_IN);
			Signature::encodeStr2Entity(sub.c_str(), tmpset);
			if(new_entity.find(objid) != new_entity.end())
			{
				it = new_sigmap.find(objid);
				if (it != new_sigmap.end())
				{
					it->second |= tmpset;
				}
				else
				{
					new_sigmap[objid] = tmpset;
				}
			}
			else
			{
				it = old_sigmap.find(objid);
				if (it != old_sigmap.end())
				{
					it->second |= tmpset;
				}
				else
				{
					old_sigmap[objid] = tmpset;
				}
			}
		}
	}

	cout<<"old sigmap size: "<<old_sigmap.size()<<endl;
	cout<<"new sigmap size: "<<new_sigmap.size()<<endl;
	cout<<"valid num: "<<valid_num<<endl;

	//NOTICE:need to sort and remove duplicates, update the valid num
	//Notice that duplicates in a group can csuse problem
	//We finish this by spo cmp

	//this->kvstore->updateTupleslist_insert(_sub_id, _pre_id, _obj_id);
	//sort and update kvstore: 11 indexes
	//
	//BETTER:maybe also use int* here with a size to start
	//NOTICE:all kvtrees are opened now, one by one if memory is bottleneck
	//
	//spo cmp: s2p s2o s2po sp2o
	{
		cout << "INSRET PROCESS: to spo cmp and update" << endl;
		qsort(id_tuples, valid_num, sizeof(int*), Database::_spo_cmp);

		//To remove duplicates
		//int ti = 1, tj = 1;
		//while(tj < valid_num)
		//{
			//if(id_tuples[tj][0] != id_tuples[tj-1][0] || id_tuples[tj][1] != id_tuples[tj-1][1] || id_tuples[tj][2] != id_tuples[tj-1][2])
			//{
				//id_tuples[ti][0] = id_tuples[tj][0];
				//id_tuples[ti][1] = id_tuples[tj][1];
				//id_tuples[ti][2] = id_tuples[tj][2];
				//ti++;
			//}
			//tj++;
		//}
		//for(tj = ti; tj < valid_num; ++tj)
		//{
			//delete[] id_tuples[tj];
			//id_tuples[tj] = NULL;
		//}
		//valid_num = ti;
		//
		//Notice that below already consider duplicates in loop

		vector<int> oidlist_s;
		vector<int> pidlist_s;
		vector<int> oidlist_sp;
		vector<int> pidoidlist_s;

		bool _sub_change = true;
		bool _sub_pre_change = true;
		bool _pre_change = true;

		for (int i = 0; i < valid_num; ++i)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				oidlist_s.push_back(_obj_id);
				oidlist_sp.push_back(_obj_id);
				pidoidlist_s.push_back(_pre_id);
				pidoidlist_s.push_back(_obj_id);
				pidlist_s.push_back(_pre_id);

				_sub_change = (i + 1 == valid_num) || (id_tuples[i][0] != id_tuples[i + 1][0]);
				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_sub_pre_change = _sub_change || _pre_change;

				if (_sub_pre_change)
				{
					cout<<"update sp2o: "<<_sub_id<<" "<<_pre_id<<" "<<oidlist_sp.size()<<endl;
					cout<<this->kvstore->getEntityByID(_sub_id)<<endl;
					cout<<this->kvstore->getPredicateByID(_pre_id)<<endl;
					this->kvstore->updateInsert_sp2o(_sub_id, _pre_id, oidlist_sp);
					oidlist_sp.clear();
				}

				if (_sub_change)
				{
					cout<<"update s2p: "<<_sub_id<<" "<<pidlist_s.size()<<endl;
					this->kvstore->updateInsert_s2p(_sub_id, pidlist_s);
					pidlist_s.clear();

					cout<<"update s2po: "<<_sub_id<<" "<<pidoidlist_s.size()<<endl;
					this->kvstore->updateInsert_s2po(_sub_id, pidoidlist_s);
					pidoidlist_s.clear();

					cout<<"update s2o: "<<_sub_id<<" "<<oidlist_s.size()<<endl;
					sort(oidlist_s.begin(), oidlist_s.end());
					this->kvstore->updateInsert_s2o(_sub_id, oidlist_s);
					oidlist_s.clear();
				}

			}
		cerr << "INSERT PROCESS: OUT s2po..." << endl;
	}
	//ops cmp: o2p o2s o2ps op2s
	{
		cout << "INSRET PROCESS: to ops cmp and update" << endl;
		qsort(id_tuples, valid_num, sizeof(int**), Database::_ops_cmp);
		vector<int> sidlist_o;
		vector<int> sidlist_op;
		vector<int> pidsidlist_o;
		vector<int> pidlist_o;

		bool _obj_change = true;
		bool _pre_change = true;
		bool _obj_pre_change = true;

		for (int i = 0; i < valid_num; ++i)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				sidlist_o.push_back(_sub_id);
				sidlist_op.push_back(_sub_id);
				pidsidlist_o.push_back(_pre_id);
				pidsidlist_o.push_back(_sub_id);
				pidlist_o.push_back(_pre_id);

				_obj_change = (i + 1 == valid_num) || (id_tuples[i][2] != id_tuples[i + 1][2]);
				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_obj_pre_change = _obj_change || _pre_change;

				if (_obj_pre_change)
				{
					cout<<"update op2s: "<<_obj_id<<" "<<_pre_id<<" "<<sidlist_op.size()<<endl;
					this->kvstore->updateInsert_op2s(_obj_id, _pre_id, sidlist_op);
					sidlist_op.clear();
				}

				if (_obj_change)
				{
					cout<<"update o2s: "<<_obj_id<<" "<<sidlist_o.size()<<endl;
					sort(sidlist_o.begin(), sidlist_o.end());
					this->kvstore->updateInsert_o2s(_obj_id, sidlist_o);
					sidlist_o.clear();

					cout<<"update o2ps: "<<_obj_id<<" "<<pidsidlist_o.size()<<endl;
					this->kvstore->updateInsert_o2ps(_obj_id, pidsidlist_o);
					pidsidlist_o.clear();

					cout<<"update o2p: "<<_obj_id<<" "<<pidlist_o.size()<<endl;
					this->kvstore->updateInsert_o2p(_obj_id, pidlist_o);
					pidlist_o.clear();
				}

			}
		cerr << "INSERT PROCESS: OUT o2ps..." << endl;
	}
	//pso cmp: p2s p2o p2so
	{
		cout << "INSRET PROCESS: to pso cmp and update" << endl;
		qsort(id_tuples, valid_num, sizeof(int*), Database::_pso_cmp);
		vector<int> sidlist_p;
		vector<int> oidlist_p;
		vector<int> sidoidlist_p;

		bool _pre_change = true;
		bool _sub_change = true;
		//bool _pre_sub_change = true;

		for (int i = 0; i < valid_num; i++)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				oidlist_p.push_back(_obj_id);
				sidoidlist_p.push_back(_sub_id);
				sidoidlist_p.push_back(_obj_id);
				sidlist_p.push_back(_sub_id);

				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_sub_change = (i + 1 == valid_num) || (id_tuples[i][0] != id_tuples[i + 1][0]);
				//_pre_sub_change = _pre_change || _sub_change;

				if (_pre_change)
				{
					cout<<"update p2s: "<<_pre_id<<" "<<sidlist_p.size()<<endl;
					this->kvstore->updateInsert_p2s(_pre_id, sidlist_p);
					sidlist_p.clear();

					cout<<"update p2o: "<<_pre_id<<" "<<oidlist_p.size()<<endl;
					sort(oidlist_p.begin(), oidlist_p.end());
					this->kvstore->updateInsert_p2o(_pre_id, oidlist_p);
					oidlist_p.clear();

					cout<<"update p2so: "<<_pre_id<<" "<<sidoidlist_p.size()<<endl;
					this->kvstore->updateInsert_p2so(_pre_id, sidoidlist_p);
					sidoidlist_p.clear();
				}
			}
		cerr << "INSERT PROCESS: OUT p2so..." << endl;
	}


	for (int i = 0; i < valid_num; ++i)
	{
		delete[] id_tuples[i];
	}
	delete[] id_tuples;

	for (it = old_sigmap.begin(); it != old_sigmap.end(); ++it)
	{
		this->vstree->updateEntry(it->first, it->second);
	}
	for (it = new_sigmap.begin(); it != new_sigmap.end(); ++it)
	{
		SigEntry _sig(it->first, it->second);
		this->vstree->insertEntry(_sig);
	}
#else
	//NOTICE:we deal with insertions one by one here
	//Callers should save the vstree(node and info) after calling this function
	for(int i = 0; i < _triple_num; ++i)
	{
		this->insertTriple(_triples[i], &_vertices, &_predicates);
	}
#endif

	//update string index
	this->stringindex->change(_vertices, *this->kvstore, true);
	this->stringindex->change(_predicates, *this->kvstore, false);

	return true;
}

bool
Database::remove(const TripleWithObjType* _triples, int _triple_num)
{
	vector<int> _vertices, _predicates;

#ifdef USE_GROUP_DELETE
	//NOTICE:this is called by remove(file) or query()(but can not be too large),
	//assume that db is loaded already
	int** id_tuples = new int*[_triple_num];
	int valid_num = 0;
	int i = 0;
	//for(i = 0; i < _triple_num; ++i)
	//{
	//id_tuples[i] = new int[3];
	//}
	//map<int, EntityBitSet> sigmap;
	//map<int, EntityBitSet>::iterator it;
	EntityBitSet tmpset;
	tmpset.reset();

	int subid, objid, preid;
	bool is_obj_entity;
	for (i = 0; i < _triple_num; ++i)
	{
		string sub = _triples[i].getSubject();
		subid = this->kvstore->getIDByEntity(sub);
		string pre = _triples[i].getPredicate();
		preid = this->kvstore->getIDByPredicate(pre);

		is_obj_entity = _triples[i].isObjEntity();
		string obj = _triples[i].getObject();
		if (is_obj_entity)
		{
			objid = this->kvstore->getIDByEntity(obj);
		}
		else //isObjLiteral
		{
			objid = this->kvstore->getIDByLiteral(obj);
		}

		if (subid == -1 || preid == -1 || objid == -1)
		{
			continue;
		}
		bool _exist_triple = this->exist_triple(subid, preid, objid);
		if (!_exist_triple)
		{
			continue;
		}

		id_tuples[valid_num] = new int[3];
		id_tuples[valid_num][0] = subid;
		id_tuples[valid_num][1] = preid;
		id_tuples[valid_num][2] = objid;
		this->triples_num--;
		valid_num++;
	}

	//NOTICE:sort and remove duplicates, update the valid num
	//Notice that duplicates in a group can cause problem

	int sub_degree, obj_degree, pre_degree;
	string tmpstr;
	//sort and update kvstore: 11 indexes
	//
	//BETTER:maybe also use int* here with a size to start
	//NOTICE:all kvtrees are opened now, one by one if memory is bottleneck
	//
	//spo cmp: s2p s2o s2po sp2o
	{
		cout << "INSRET PROCESS: to spo cmp and update" << endl;
		qsort(id_tuples, valid_num, sizeof(int*), Database::_spo_cmp);
		vector<int> oidlist_s;
		vector<int> pidlist_s;
		vector<int> oidlist_sp;
		vector<int> pidoidlist_s;

		bool _sub_change = true;
		bool _sub_pre_change = true;
		bool _pre_change = true;

		for (int i = 0; i < valid_num; ++i)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				oidlist_s.push_back(_obj_id);
				oidlist_sp.push_back(_obj_id);
				pidoidlist_s.push_back(_pre_id);
				pidoidlist_s.push_back(_obj_id);
				pidlist_s.push_back(_pre_id);

				_sub_change = (i + 1 == valid_num) || (id_tuples[i][0] != id_tuples[i + 1][0]);
				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_sub_pre_change = _sub_change || _pre_change;

				if (_sub_pre_change)
				{
					this->kvstore->updateRemove_sp2o(_sub_id, _pre_id, oidlist_sp);
					oidlist_sp.clear();
				}

				if (_sub_change)
				{
					this->kvstore->updateRemove_s2p(_sub_id, pidlist_s);
					pidlist_s.clear();
					this->kvstore->updateRemove_s2po(_sub_id, pidoidlist_s);
					pidoidlist_s.clear();

					sort(oidlist_s.begin(), oidlist_s.end());
					this->kvstore->updateRemove_s2o(_sub_id, oidlist_s);
					oidlist_s.clear();

					sub_degree = (this->kvstore)->getEntityDegree(_sub_id);
					if (sub_degree == 0)
					{
						tmpstr = this->kvstore->getEntityByID(_sub_id);
						this->kvstore->subEntityByID(_sub_id);
						this->kvstore->subIDByEntity(tmpstr);
						(this->vstree)->removeEntry(_sub_id);
						this->freeEntityID(_sub_id);
						this->sub_num--;
					}
					else
					{
						tmpset.reset();
						this->calculateEntityBitSet(_sub_id, tmpset);
						this->vstree->replaceEntry(_sub_id, tmpset);
					}
				}

			}
		cerr << "INSERT PROCESS: OUT s2po..." << endl;
	}
	//ops cmp: o2p o2s o2ps op2s
	{
		cout << "INSRET PROCESS: to ops cmp and update" << endl;
		qsort(id_tuples, valid_num, sizeof(int**), Database::_ops_cmp);
		vector<int> sidlist_o;
		vector<int> sidlist_op;
		vector<int> pidsidlist_o;
		vector<int> pidlist_o;

		bool _obj_change = true;
		bool _pre_change = true;
		bool _obj_pre_change = true;

		for (int i = 0; i < valid_num; ++i)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				sidlist_o.push_back(_sub_id);
				sidlist_op.push_back(_sub_id);
				pidsidlist_o.push_back(_pre_id);
				pidsidlist_o.push_back(_sub_id);
				pidlist_o.push_back(_pre_id);

				_obj_change = (i + 1 == valid_num) || (id_tuples[i][2] != id_tuples[i + 1][2]);
				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_obj_pre_change = _obj_change || _pre_change;

				if (_obj_pre_change)
				{
					this->kvstore->updateRemove_op2s(_obj_id, _pre_id, sidlist_op);
					sidlist_op.clear();
				}

				if (_obj_change)
				{
					sort(sidlist_o.begin(), sidlist_o.end());
					this->kvstore->updateRemove_o2s(_obj_id, sidlist_o);
					sidlist_o.clear();
					this->kvstore->updateRemove_o2ps(_obj_id, pidsidlist_o);
					pidsidlist_o.clear();

					this->kvstore->updateRemove_o2p(_obj_id, pidlist_o);
					pidlist_o.clear();

					is_obj_entity = this->objIDIsEntityID(_obj_id);
					if(is_obj_entity) 
					{
						obj_degree = this->kvstore->getEntityDegree(_obj_id);
						if (obj_degree == 0)
						{
							tmpstr = this->kvstore->getEntityByID(_obj_id);
							this->kvstore->subEntityByID(_obj_id);
							this->kvstore->subIDByEntity(tmpstr);
							(this->vstree)->removeEntry(_obj_id);
							this->freeEntityID(_obj_id);
						}
						else
						{
							tmpset.reset();
							this->calculateEntityBitSet(_obj_id, tmpset);
							this->vstree->replaceEntry(_obj_id, tmpset);
						}
					}
					else
					{
						obj_degree = this->kvstore->getLiteralDegree(_obj_id);
						if (obj_degree == 0)
						{
							tmpstr = this->kvstore->getLiteralByID(_obj_id);
							this->kvstore->subLiteralByID(_obj_id);
							this->kvstore->subIDByLiteral(tmpstr);
							this->freeLiteralID(_obj_id);
						}
					}
				}

			}
		cerr << "INSERT PROCESS: OUT o2ps..." << endl;
	}
	//pso cmp: p2s p2o p2so
	{
		cout << "INSRET PROCESS: to pso cmp and update" << endl;
		qsort(id_tuples, valid_num, sizeof(int*), Database::_pso_cmp);
		vector<int> sidlist_p;
		vector<int> oidlist_p;
		vector<int> sidoidlist_p;

		bool _pre_change = true;
		bool _sub_change = true;
		//bool _pre_sub_change = true;

		for (int i = 0; i < valid_num; i++)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				oidlist_p.push_back(_obj_id);
				sidoidlist_p.push_back(_sub_id);
				sidoidlist_p.push_back(_obj_id);
				sidlist_p.push_back(_sub_id);

				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_sub_change = (i + 1 == valid_num) || (id_tuples[i][0] != id_tuples[i + 1][0]);
				//_pre_sub_change = _pre_change || _sub_change;

				if (_pre_change)
				{
					this->kvstore->updateRemove_p2s(_pre_id, sidlist_p);
					sidlist_p.clear();

					sort(oidlist_p.begin(), oidlist_p.end());
					this->kvstore->updateRemove_p2o(_pre_id, oidlist_p);
					oidlist_p.clear();

					this->kvstore->updateRemove_p2so(_pre_id, sidoidlist_p);
					sidoidlist_p.clear();

					pre_degree = this->kvstore->getPredicateDegree(_pre_id);
					if (pre_degree == 0)
					{
						tmpstr = this->kvstore->getPredicateByID(_pre_id);
						this->kvstore->subPredicateByID(_pre_id);
						this->kvstore->subIDByPredicate(tmpstr);
						this->freePredicateID(_pre_id);
					}
				}
			}
		cerr << "INSERT PROCESS: OUT p2so..." << endl;
	}


	for (int i = 0; i < valid_num; ++i)
	{
		delete[] id_tuples[i];
	}
	delete[] id_tuples;
#else
	//NOTICE:we deal with deletions one by one here
	//Callers should save the vstree(node and info) after calling this function
	for(int i = 0; i < _triple_num; ++i)
	{
		this->removeTriple(_triples[i], &_vertices, &_predicates);
	}
#endif

	//update string index
	this->stringindex->disable(_vertices, true);
	this->stringindex->disable(_predicates, false);

	return true;
}

bool
Database::objIDIsEntityID(int _id)
{
	return _id < Util::LITERAL_FIRST_ID;
}

bool
Database::getFinalResult(SPARQLquery& _sparql_q, ResultSet& _result_set)
{
#ifdef DEBUG_PRECISE
	printf("getFinalResult:begins\n");
#endif
	// this is only selected var num
	int _var_num = _sparql_q.getQueryVarNum();
	_result_set.setVar(_sparql_q.getQueryVar());
	vector<BasicQuery*>& query_vec = _sparql_q.getBasicQueryVec();

	//sum the answer number
	int _ans_num = 0;
#ifdef DEBUG_PRECISE
	printf("getFinalResult:before ansnum loop\n");
#endif
	for (unsigned i = 0; i < query_vec.size(); i++)
	{
		_ans_num += query_vec[i]->getResultList().size();
	}
#ifdef DEBUG_PRECISE
	printf("getFinalResult:after ansnum loop\n");
#endif

	_result_set.ansNum = _ans_num;
#ifndef STREAM_ON
	_result_set.answer = new string*[_ans_num];
	for (int i = 0; i < _result_set.ansNum; i++)
	{
		_result_set.answer[i] = NULL;
	}
#else
	vector<int> keys;
	vector<bool> desc;
	_result_set.openStream(keys, desc, 0, -1);
#ifdef DEBUG_PRECISE
	printf("getFinalResult:after open stream\n");
#endif
#endif
#ifdef DEBUG_PRECISE
	printf("getFinalResult:before main loop\n");
#endif
	int tmp_ans_count = 0;
	//map int ans into string ans
	//union every basic result into total result
	for (unsigned i = 0; i < query_vec.size(); i++)
	{
		vector<int*>& tmp_vec = query_vec[i]->getResultList();
		//ensure the spo order is right, but the triple order is still reversed
		//for every result group in resultlist
		//for(vector<int*>::reverse_iterator itr = tmp_vec.rbegin(); itr != tmp_vec.rend(); ++itr)
		for (vector<int*>::iterator itr = tmp_vec.begin(); itr != tmp_vec.end(); ++itr)
		{
			//to ensure the order so do reversely in two nested loops
#ifndef STREAM_ON
			_result_set.answer[tmp_ans_count] = new string[_var_num];
#endif
#ifdef DEBUG_PRECISE
			printf("getFinalResult:before map loop\n");
#endif
			//NOTICE: in new join method only selec_var_num columns,
			//but before in shenxuchuan's join method, not like this.
			//though there is all graph_var_num columns in result_list,
			//we only consider the former selected vars
			//map every ans_id into ans_str
			for (int v = 0; v < _var_num; ++v)
			{
				int ans_id = (*itr)[v];
				string ans_str;
				if (this->objIDIsEntityID(ans_id))
				{
					ans_str = (this->kvstore)->getEntityByID(ans_id);
				}
				else
				{
					ans_str = (this->kvstore)->getLiteralByID(ans_id);
				}
#ifndef STREAM_ON
				_result_set.answer[tmp_ans_count][v] = ans_str;
#else
				_result_set.writeToStream(ans_str);
#endif
#ifdef DEBUG_PRECISE
				printf("getFinalResult:after copy/write\n");
#endif
			}
			tmp_ans_count++;
		}
	}
#ifdef STREAM_ON
	_result_set.resetStream();
#endif
#ifdef DEBUG_PRECISE
	printf("getFinalResult:ends\n");
#endif

	return true;
}


void
Database::printIDlist(int _i, int* _list, int _len, string _log)
{
	stringstream _ss;
	_ss << "[" << _i << "] ";
	for (int i = 0; i < _len; i++) {
		_ss << _list[i] << "\t";
	}
	Util::logging("==" + _log + ":");
	Util::logging(_ss.str());
}

void
Database::printPairList(int _i, int* _list, int _len, string _log)
{
	stringstream _ss;
	_ss << "[" << _i << "] ";
	for (int i = 0; i < _len; i += 2) {
		_ss << "[" << _list[i] << "," << _list[i + 1] << "]\t";
	}
	Util::logging("==" + _log + ":");
	Util::logging(_ss.str());
}

void
Database::test()
{
	int subNum = 9, preNum = 20, objNum = 90;

	int* _id_list = NULL;
	int _list_len = 0;
	{   /* x2ylist */
		for (int i = 0; i < subNum; i++)
		{

			(this->kvstore)->getobjIDlistBysubID(i, _id_list, _list_len);
			if (_list_len != 0)
			{
				stringstream _ss;
				this->printIDlist(i, _id_list, _list_len, "s2olist[" + _ss.str() + "]");
				delete[] _id_list;
			}

			/* o2slist */
			(this->kvstore)->getsubIDlistByobjID(i, _id_list, _list_len);
			if (_list_len != 0)
			{
				stringstream _ss;
				this->printIDlist(i, _id_list, _list_len, "o(sub)2slist[" + _ss.str() + "]");
				delete[] _id_list;
			}
		}

		for (int i = 0; i < objNum; i++)
		{
			int _i = Util::LITERAL_FIRST_ID + i;
			(this->kvstore)->getsubIDlistByobjID(_i, _id_list, _list_len);
			if (_list_len != 0)
			{
				stringstream _ss;
				this->printIDlist(_i, _id_list, _list_len, "o(literal)2slist[" + _ss.str() + "]");
				delete[] _id_list;
			}
		}
	}
	{   /* xy2zlist */
		for (int i = 0; i < subNum; i++)
		{
			for (int j = 0; j < preNum; j++)
			{
				(this->kvstore)->getobjIDlistBysubIDpreID(i, j, _id_list,
					_list_len);
				if (_list_len != 0)
				{
					stringstream _ss;
					_ss << "preid:" << j;
					this->printIDlist(i, _id_list, _list_len, "sp2olist[" + _ss.str() + "]");
					delete[] _id_list;
				}

				(this->kvstore)->getsubIDlistByobjIDpreID(i, j, _id_list,
					_list_len);
				if (_list_len != 0)
				{
					stringstream _ss;
					_ss << "preid:" << j;
					this->printIDlist(i, _id_list, _list_len, "o(sub)p2slist[" + _ss.str() + "]");
					delete[] _id_list;
				}
			}
		}

		for (int i = 0; i < objNum; i++)
		{
			int _i = Util::LITERAL_FIRST_ID + i;
			for (int j = 0; j < preNum; j++)
			{
				(this->kvstore)->getsubIDlistByobjIDpreID(_i, j, _id_list,
					_list_len);
				if (_list_len != 0)
				{
					stringstream _ss;
					_ss << "preid:" << j;
					this->printIDlist(_i, _id_list, _list_len,
						"*o(literal)p2slist[" + _ss.str() + "]");
					delete[] _id_list;
				}
			}
		}
	}
	{   /* x2yzlist */
		for (int i = 0; i < subNum; i++)
		{
			(this->kvstore)->getpreIDobjIDlistBysubID(i, _id_list, _list_len);
			if (_list_len != 0)
			{
				this->printPairList(i, _id_list, _list_len, "s2polist");
				delete[] _id_list;
				_list_len = 0;
			}
		}

		for (int i = 0; i < subNum; i++)
		{
			(this->kvstore)->getpreIDsubIDlistByobjID(i, _id_list, _list_len);
			if (_list_len != 0)
			{
				this->printPairList(i, _id_list, _list_len, "o(sub)2pslist");
				delete[] _id_list;
			}
		}

		for (int i = 0; i < objNum; i++)
		{
			int _i = Util::LITERAL_FIRST_ID + i;
			(this->kvstore)->getpreIDsubIDlistByobjID(_i, _id_list, _list_len);
			if (_list_len != 0)
			{
				this->printPairList(_i, _id_list, _list_len,
					"o(literal)2pslist");
				delete[] _id_list;
			}
		}
	}
}

void
Database::test_build_sig()
{
	BasicQuery* _bq = new BasicQuery("");
	/*
	* <!!!>	y:created	<!!!_(album)>.
	*  <!!!>	y:created	<Louden_Up_Now>.
	*  <!!!_(album)>	y:hasSuccessor	<Louden_Up_Now>
	* <!!!_(album)>	rdf:type	<wordnet_album_106591815>
	*
	* id of <!!!> is 0
	* id of <!!!_(album)> is 2
	*
	*
	* ?x1	y:created	?x2.
	*  ?x1	y:created	<Louden_Up_Now>.
	*  ?x2	y:hasSuccessor	<Louden_Up_Now>.
	* ?x2	rdf:type	<wordnet_album_106591815>
	*/
	{
		Triple _triple("?x1", "y:created", "?x2");
		_bq->addTriple(_triple);
	}
	{
		Triple _triple("?x1", "y:created", "<Louden_Up_Now>");
		_bq->addTriple(_triple);
	}
	{
		Triple _triple("?x2", "y:hasSuccessor", "<Louden_Up_Now>");
		_bq->addTriple(_triple);
	}
	{
		Triple _triple("?x2", "rdf:type", "<wordnet_album_106591815>");
		_bq->addTriple(_triple);
	}
	vector<string> _v;
	_v.push_back("?x1");
	_v.push_back("?x2");

	_bq->encodeBasicQuery(this->kvstore, _v);
	Util::logging(_bq->to_str());
	SPARQLquery _q;
	_q.addBasicQuery(_bq);

	(this->vstree)->retrieve(_q);

	Util::logging("\n\n");
	Util::logging("candidate:\n\n" + _q.candidate_str());
}

bool
Database::locallyJoin(vector< vector<int> >& candidates_vec, vector< set<int> >& can_set_list, vector<string>& lpm_str_vec, vector< vector<int> >& res_crossing_edges_vec, vector<int>& all_crossing_edges_vec, vector< vector<int> > &_query_dir_ad, vector< vector<int> > &_query_pre_ad, vector< vector<int> > &_query_ad, set<int>& satellites_set, int myRank, vector< set<int> >& internal_can_set_list)
{
	int this_var_num = _query_ad.size();
	set<RecordType> cur_partial_matches;
	TableType this_current_table;
	for(int _start_idx = 0; _start_idx < this_var_num; _start_idx++){
		
		if(satellites_set.count(_start_idx) != 0){
			continue;
		}
		
		int this_start_id = _start_idx;
		//vector<bool> this_dealed_triple(this_edge_num, false);
		stack<RecordType> this_mystack;
		for(int i = 0; i < candidates_vec[this_start_id].size(); ++i)
		{
			int ele = candidates_vec[this_start_id][i];
			if(ele >= Util::LITERAL_FIRST_ID || this->internal_tag_str.at(ele) == '1'){
				RecordType record(this_var_num, -1);
				record[this_start_id] = ele;
				this_mystack.push(record);
			}
		}
		
		while(!this_mystack.empty()){
			
			RecordType record = this_mystack.top();
			this_mystack.pop();
			
			set<int> dealed_id;
			int next_id = this->choose_next_node(record, _query_ad, dealed_id);
			if(next_id == -1){
				this_current_table.push_back(record);
				continue;
			}
			
			IDList* valid_ans_list = NULL;
			bool matched = true;
			
			for(int i = 0; i < _query_ad[next_id].size(); i++){
				
				if(dealed_id.count(_query_ad[next_id][i]) != 0){
					if(_query_pre_ad[next_id][i] == -1){
						matched = false;
						break;
					}
					
					int ele = record[_query_ad[next_id][i]];
					
					int* id_list;
					int id_list_len = -1;
					if(_query_dir_ad[next_id][i] == 0){
						if(_query_pre_ad[next_id][i] == -2)
							this->kvstore->getsubIDlistByobjID(ele, id_list, id_list_len);
						else if(_query_pre_ad[next_id][i] >= 0)
							this->kvstore->getsubIDlistByobjIDpreID(ele, _query_pre_ad[next_id][i], id_list, id_list_len);
															
						//this->kvstore->getsubIDlistByobjIDpreID(ele, _query_pre_ad[next_id][i], id_list, id_list_len);
					}else{
						if(_query_pre_ad[next_id][i] == -2)
							this->kvstore->getobjIDlistBysubID(ele, id_list, id_list_len);
						else if(_query_pre_ad[next_id][i] >= 0)
							this->kvstore->getobjIDlistBysubIDpreID(ele, _query_pre_ad[next_id][i], id_list, id_list_len);
						
						//this->kvstore->getobjIDlistBysubIDpreID(ele, _query_pre_ad[next_id][i], id_list, id_list_len);
					}
					
					if(id_list_len == -1 || id_list_len == 0)
					{
						matched = false;
						break;
					}
					
					if(valid_ans_list == NULL)
					{
						valid_ans_list = new IDList;
						for(int i = 0; i < id_list_len; ++i){
							valid_ans_list->addID(id_list[i]);
						}	
					}else{
						valid_ans_list->intersectList(id_list, id_list_len);
					}
					
					if(valid_ans_list->size() == 0)
					{
						matched = false;
						break;
					}
					
					delete[] id_list;
				}
			}
			
			if(matched)
			{
				for(int i = 0; i < valid_ans_list->size(); ++i)
				{
					RecordType tmp(record);
					tmp[next_id] = (*valid_ans_list)[i];
					
					if(tmp[next_id] >= Util::LITERAL_FIRST_ID || internal_tag_str.at(tmp[next_id]) == '1'){
						if(internal_can_set_list[next_id].count(tmp[next_id]) != 0)
							this_mystack.push(tmp);
					}else{
						string tmp_extended_node_str = (this->kvstore)->getEntityByID(tmp[next_id]);
						int tmp_hash_val = Util::BKDRHash(tmp_extended_node_str.c_str());
						if(tmp_hash_val < 0)
							tmp_hash_val *= -1;
						tmp_hash_val = tmp_hash_val % Util::MAX_CROSSING_EDGE_HASH_SIZE;
						
						if(can_set_list[next_id].count(tmp_hash_val) != 0){
							this_mystack.push(tmp);
						}
					}
				}
			}
			delete valid_ans_list;
			valid_ans_list = NULL;
		}
		cur_partial_matches.insert(this_current_table.begin(), this_current_table.end());
		
		this_current_table.clear();
	}
	this_current_table.assign(cur_partial_matches.begin(), cur_partial_matches.end());
	//printf("In Client %d, there are %d local partial matches\n", myRank, this_current_table.size());
	
	for(TableIterator it0 = this_current_table.begin(); it0 != this_current_table.end(); ++it0)
	{
		RecordType cur_record = *it0;
		int size = cur_record.size();
		
		vector<int> tmp_crossing_edge_vec;
		vector<string> tmp_res_vec(size, "");
		vector<char> tmp_res_tag_vec(size, '0');
		
		//printf("result_str2id.size() = %d\n", result_str2id.size());
		for (int v = 0; v < cur_record.size(); ++v)
		{
			if (cur_record[v] != -1)
			{	
				if(cur_record[v] >= Util::LITERAL_FIRST_ID){
					tmp_res_tag_vec[v] = '1';
					tmp_res_vec[v] = (this->kvstore)->getLiteralByID(cur_record[v]);
				}else if(internal_tag_str.at(cur_record[v]) == 1){
					tmp_res_tag_vec[v] = '1';
					tmp_res_vec[v] = (this->kvstore)->getEntityByID(cur_record[v]);
				}else{
					if(satellites_set.count(v) == 0){
						tmp_res_tag_vec[v] = internal_tag_str.at(cur_record[v]);
						tmp_res_vec[v] = (this->kvstore)->getEntityByID(cur_record[v]);
					}else{
						tmp_res_tag_vec[v] = '1';
						tmp_res_vec[v] = (this->kvstore)->getEntityByID(cur_record[v]);
					}
				}
			}else{
				tmp_res_tag_vec[v] = '2';
				tmp_res_vec[v] = "-1";
			}
		}
		
		stringstream partial_res_ss;
		for (int v = 0; v < cur_record.size(); ++v){
			int var_id = v;
			if(tmp_res_tag_vec[var_id] != '2')
				partial_res_ss << tmp_res_tag_vec[var_id];
			partial_res_ss << tmp_res_vec[var_id] << "\t";
			
			if(tmp_res_tag_vec[var_id] != '1'){
				continue;
			}
			
			for (int k = 0; k < _query_ad[v].size(); k++){
				int var_id2 = _query_ad[v][k];
				
				if (var_id2 == -1)
				{
					continue;
				}
				
				if(tmp_res_vec[var_id2].compare("-1") != 0){
					if(tmp_res_tag_vec[var_id2] == '0' && satellites_set.count(var_id2) == 0){
						stringstream crossing_edge_ss;
						
						if(var_id < var_id2){
							crossing_edge_ss << var_id << "\t" << var_id2 << "\t" << tmp_res_vec[var_id] << "\t" << tmp_res_vec[var_id2] << "\t";
						}else{
							crossing_edge_ss << var_id2 << "\t" << var_id << "\t" << tmp_res_vec[var_id2] << "\t" << tmp_res_vec[var_id] << "\t";
						}
						
						int tmp_hash_val = Util::BKDRHash(crossing_edge_ss.str().c_str());
						if(tmp_hash_val < 0)
							tmp_hash_val *= -1;
						
						all_crossing_edges_vec.push_back((tmp_hash_val % Util::MAX_CROSSING_EDGE_HASH_SIZE));
						tmp_crossing_edge_vec.push_back((tmp_hash_val % Util::MAX_CROSSING_EDGE_HASH_SIZE));
						
						//log_output_partial << crossing_edge_ss.str() << "=" << (tmp_hash_val % Util::MAX_CROSSING_EDGE_HASH_SIZE) << "\t";
					}
				}
			}
		}
		res_crossing_edges_vec.push_back(tmp_crossing_edge_vec);
		partial_res_ss << endl;
		lpm_str_vec.push_back(partial_res_ss.str());
	}
	
    return true;
}


int
Database::choose_next_node(RecordType& record, vector< vector<int> > &_query_ad, set<int>& dealed_id)
{
	set<int> matched_id;
	for(int i = 0; i < _query_ad.size(); i++){
		if(record[i] != -1){
			if(record[i] >= Util::LITERAL_FIRST_ID || this->internal_tag_str.at(record[i]) == '1'){
				dealed_id.insert(i);
			}
			matched_id.insert(i);
		}
	}
	
	for(int i = 0; i < _query_ad.size(); i++){
		if(dealed_id.count(i) != 0){
			for(int j = 0; j < _query_ad[i].size(); j++){
				if(matched_id.count(_query_ad[i][j]) == 0){
					return _query_ad[i][j];
				}
			}
		}
	}
	
    return -1;
}
