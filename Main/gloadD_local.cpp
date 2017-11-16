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

using namespace std;

//[0]./gload [1]data_folder_path  [2]rdf_file_path
int 
main(int argc, char * argv[])
{
	//chdir(dirname(argv[0]));
	Util util;
		
	string _db_path = string(argv[1]);
	Database _db(_db_path);
	bool flag = _db.build("_distributed_gStore_tmp_rdf_triples.n3", "_distributed_gStore_tmp_internal_vertices.txt");
	if (flag)
	{
		cout << "import RDF file to database done." << endl;
	}
	else
	{
		cout << "import RDF file to database failed." << endl;
	}
		
	return 0;
}