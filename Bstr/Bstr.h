/*=============================================================================
# Filename: Bstr.h
# Author: Bookug Lobert 
# Mail: 1181955272@qq.com
# Last Modified: 2015-10-16 13:01
# Description: 
1. firstly written by liyouhuan, modified by zengli
2. class declaration for Bstr(used to store arbitary string) 
=============================================================================*/


#ifndef _BSTR_BSTR_H
#define _BSTR_BSTR_H

#include "../Util/Util.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

class Bstr
{
private:
	char* str;		//pointers consume 8 byte in 64-bit system
	unsigned length;

public:
	Bstr();
	//if copy memory, then use const char*, but slow
	//else, can not use const char* -> char*
	Bstr(const char* _str, unsigned _len);
	//Bstr(char* _str, unsigned _len);
	Bstr(const Bstr& _bstr);
	//Bstr& operate = (const Bstr& _bstr);

	bool operator > (const Bstr& _bstr);
	bool operator < (const Bstr& _bstr);
	bool operator == (const Bstr& _bstr);
	bool operator <= (const Bstr& _bstr);
	bool operator >= (const Bstr& _bstr);
	bool operator != (const Bstr& _bstr);
	unsigned getLen() const;
	void setLen(unsigned _len);
	char* getStr() const;
	void setStr(char* _str);		//reuse a TBstr
	void release();					//release memory
	void clear();					//set str/length to 0
	void copy(const Bstr* _bp);
	void copy(const char* _str, unsigned _len);
	//bool read(FILE* _fp);
	//int write(FILE* _fp);
	~Bstr();
	void print(std::string s) const;		//DEBUG
};

#endif // _BSTR_BSTR_H

