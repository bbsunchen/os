#include <stdio.h>
#include <list>
using namespace std;

int main()
{
	list<int> a;
	for (int i = 1; i <=5 ; i++)
		a.push_back(i*10);
	
	for (auto & it: a)
		printf("%d \n", it);
	return 0;
}
