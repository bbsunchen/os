#include "connecter.hh"

class Pck_Ut{
public:
	static void set_pck(pck_t *, string, string, int*, int, int, struct pfs_stat *, char *);
	static void cp_op(pck_t *, string);
	static void cp_fn(pck_t *, string);
	static void cp_bId(pck_t *, int *);
	static void cp_sw(pck_t *, int);
	static void cp_fd(pck_t *, int);
	static void cp_st(pck_t *, struct pfs_stat *);
	static void cp_dt(pck_t *, char *);
};