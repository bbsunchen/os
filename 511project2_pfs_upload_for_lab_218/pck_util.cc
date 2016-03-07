#include "pck_util.hh"
void Pck_Ut::set_pck(pck_t * pck, string op , string fn, int* bid, int sw, int fd, struct pfs_stat* st, char * dt)
{
  cp_op( pck, op);
  cp_fn( pck, fn);
  cp_bId(pck, bid);
  cp_sw( pck, sw);
  cp_fd( pck, fd);
  cp_st( pck, st);
  cp_dt( pck, dt);
};

void Pck_Ut::cp_op(pck_t * pck, string op)
{
  memcpy(pck->opcode, op.c_str(), op.length());
    (pck->opcode)[op.length()] = '\0';  
};

void Pck_Ut::cp_fn(pck_t * pck, string fn)
{
    memcpy(pck->filename, fn.c_str(), fn.length());
    (pck->filename)[fn.length()] = '\0';
};

void Pck_Ut::cp_bId(pck_t * pck, int * bid)
{
  pck->blockId[0] = bid[0];
  pck->blockId[1] = bid[1];
};
void Pck_Ut::cp_sw(pck_t * pck, int sw)
{
  pck->stripe_width = sw;
};
void Pck_Ut::cp_fd(pck_t * pck, int fd)
{
  pck->fd = fd;
};
void Pck_Ut::cp_st(pck_t * pck, struct pfs_stat * st)
{
  memcpy(&(pck->pstat), st, sizeof(struct pfs_stat));
};
void Pck_Ut::cp_dt(pck_t * pck, char * d)
{
  if (d == NULL)  memset(&(pck->data), '\0', PFS_BLOCK_SIZE_BYTE);
  else memcpy(&(pck->data), d, PFS_BLOCK_SIZE_BYTE);
}
