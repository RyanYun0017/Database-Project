#include "projectA.h"
#include "projectB.h"


int insertmeta[100]={0};
int getmeta[100]={0};
int sdmeta[100]={0};
int scanmeta[100][100]={{0}};

void HFL_init() 
{
    fprintf(stderr, "Calling HFL_init\n");
    BM_init();
    for (int i = 0; i < 100; i++) {        // This loop is used to initialize the sdmeta
        sdmeta[i] = -1;
    }
    
    for (int i = 0;i< 100; i++) {
        for ( int j = 0; j < 100; j++) {   // This double loop is used to initialize the scanmeta
            scanmeta[i][j] = 0;
        }
    }
}


errCode HFL_create_file(char* filename)
{
    fprintf(stderr, "Attempting to create file with name %s\n", filename);
    FILE *fp;
    fp=fopen(filename, "w+");
    if (fp == NULL) {
        printf("Fail to create file");
        return 0;
    }
    return 0;
}


fileDesc HFL_open_file(char* filename)
{
    fprintf(stderr, "Opening file with name %s\n", filename);
    int fd;
    FILE *fp;
    fp=fopen(filename, "rb+");
    if (fp==NULL) {
        printf("Cannot open this file");/* To check if the file is opened successfully */
        return 0;
    }
    fd=fileno(fp); /* return a file descriptor fd.*/
    return fd;
}


errCode HFL_close_file(fileDesc fd)
{
    fprintf(stderr, "Attempting to close file %d\n", fd);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    fclose(fp);
    return 0;
}


recordID HFL_insert_rec(fileDesc fd, record* rec)
{
    fprintf(stderr, "Attempting to insert record into file %d\n", fd);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    
    block *blockPtr=NULL;
    
    int rid=insertmeta[fd];
    
    int bid=(rid/256)+1;  /*Compute the block id. Plus 1 is because my block id is start form 1  */
    
    int tid=(rid % 256)*16;
    printf("tid is %d\n",tid/16);
    int i=0;
    while (i < 100) {
        if (bid==bufferpool[i].blockID && fd==bufferpool[i].fd) {
            
            bufferpool[i].data[tid]=(rec->attribute1);
            bufferpool[i].data[tid+sizeof(int)]=(rec->attribute2);
            bufferpool[i].data[tid+sizeof(int)*2]=(rec->attribute3);
            bufferpool[i].data[tid+12]=rec->attribute4;
            bufferpool[i].data[tid+13]=rec->attribute5;
            bufferpool[i].data[tid+14]=rec->attribute6;
            bufferpool[i].data[tid+15]=rec->attribute7;
            insertmeta[fd]++;
            printf("The data is %c\n",bufferpool[i].data[tid+15]);
            printf("The record %d has been insert to blcok %d in bufferpool %d\n",rid,bid,i);
            break;

            }
        i++;
    }
    printf("i equals to %d\n",i);
    
    if (i==100) {
        printf("Cannot find %d block in bufferpool, we shoud get it from file\n",bid);
        BM_get_this_block(fd, bid, &blockPtr);
        blockPtr->data[tid]=rec->attribute1;
        blockPtr->data[tid+4]=rec->attribute2;
        blockPtr->data[tid+8]=rec->attribute3;
        blockPtr->data[tid+12]=rec->attribute4;
        blockPtr->data[tid+13]=rec->attribute5;
        blockPtr->data[tid+14]=rec->attribute6;
        blockPtr->data[tid+15]=rec->attribute7;
        
        insertmeta[fd]++;
        printf("The data is %c\n",bufferpool[lastuse].data[tid+15]);
    }
    
    
    return rid;
}

errCode HFL_delete_rec(fileDesc fd, recordID rid)
{
    fprintf(stderr, "Attempting to delete record with ID %d from file %d\n", rid, fd);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    
    block *blockPtr=NULL;
    
    int bid=(rid/256)+1;
    
    int tid=(rid % 256)*16;
    
    int i=0;
    while (i < 100) {
        if (bid==bufferpool[i].blockID && fd==bufferpool[i].fd) {
            /*Compute the position of record in this blick, then insert record to it.*/
            
            bufferpool[i].data[tid]=0;
            bufferpool[i].data[tid+sizeof(int)]='\0';
            bufferpool[i].data[tid+sizeof(int)*2]='\0';
            bufferpool[i].data[tid+12]='\0';
            bufferpool[i].data[tid+13]='\0';
            bufferpool[i].data[tid+14]='\0';
            bufferpool[i].data[tid+15]='\0';
            
            printf("The data is %c\n",bufferpool[i].data[tid+15]);
            printf("The record %d has been delete in blcok %d in bufferpool %d\n",rid,bid,i);
            
            break;
        }
        i++;
    }
    printf("i equals to %d\n",i);
    
    if (i==100) {
        printf("Cannot find %d block in bufferpool, we shoud delete it from file\n",bid);
        BM_get_this_block(fd, bid, &blockPtr);
        blockPtr->data[tid]='\0';
        blockPtr->data[tid+4]='\0';
        blockPtr->data[tid+8]='\0';
        blockPtr->data[tid+12]='\0';
        blockPtr->data[tid+13]='\0';
        blockPtr->data[tid+14]='\0';
        blockPtr->data[tid+15]='\0';
        
       
        printf("The data is %c\n",bufferpool[0].data[tid+15]);
    }

    
    return 0;
    
}


errCode HFL_get_first_rec(fileDesc fd, record** rec)
{
    fprintf(stderr, "Attempting to get first record from file %d\n", fd);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    int bid=1;
    int tid=0;
    int i=0;
    *rec=malloc(sizeof(record));
    
    block* blockPtr=NULL;
    
    while (i<100) {
        if (bid == bufferpool[i].blockID && fd == bufferpool[i].fd) {
            
            (*rec)->attribute1 = bufferpool[i].data[tid];
            (*rec)->attribute2 = bufferpool[i].data[tid+4];
            (*rec)->attribute3 = bufferpool[i].data[tid+8];
            (*rec)->attribute4 = bufferpool[i].data[tid+12];
            (*rec)->attribute5 = bufferpool[i].data[tid+13];
            (*rec)->attribute6 = bufferpool[i].data[tid+14];
            (*rec)->attribute7 = bufferpool[i].data[tid+15];
            getmeta[fd]=0;
            
            
            break;
        }
        i++;
    }
    
    printf("i equals to %d\n",i);
    
    if (i == 100) {
        BM_get_this_block(fd, bid, &blockPtr);
        (*rec)->attribute1 = blockPtr->data[tid];
        (*rec)->attribute2 = blockPtr->data[tid+4];
        (*rec)->attribute3 = blockPtr->data[tid+8];
        (*rec)->attribute4 = blockPtr->data[tid+12];
        (*rec)->attribute5 = blockPtr->data[tid+13];
        (*rec)->attribute6 = blockPtr->data[tid+14];
        (*rec)->attribute7 = blockPtr->data[tid+15];
        getmeta[fd]=0;
        
    }
    
    return 0;
}

errCode HFL_get_next_rec(fileDesc fd, record** rec)
{
    fprintf(stderr, "Attempting to get next record from file %d\n", fd);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    
    int rid=getmeta[fd]+1; //That is because I use the getmeta array to store the last used record id.
    int bid=rid/256+1;  // block ID should start from 1
    int tid=(rid % 256)*16;
    block* blockPtr=NULL;
    (*rec)=malloc(sizeof(record));
    
    int i=0;
    while (i<100) {
        if (bid == bufferpool[i].blockID && fd == bufferpool[i].fd) {
            printf("GNR- find the record in bufferpoor %d\n",i);
            (*rec)->attribute1 = bufferpool[i].data[tid];
            (*rec)->attribute2 = bufferpool[i].data[tid+4];
            (*rec)->attribute3 = bufferpool[i].data[tid+8];
            (*rec)->attribute4 = bufferpool[i].data[tid+12];
            (*rec)->attribute5 = bufferpool[i].data[tid+13];
            (*rec)->attribute6 = bufferpool[i].data[tid+14];
            (*rec)->attribute7 = bufferpool[i].data[tid+15];
            getmeta[fd]=rid; // Updata the meta data which stores last used record id.
            break;
        }
        i++;
    }
    
    if (i == 100) {
        
        printf("GNR-find record in file fd=%d\n",fd);
        BM_get_this_block(fd, bid, &blockPtr);
        (*rec)->attribute1 = blockPtr->data[tid];
        (*rec)->attribute2 = blockPtr->data[tid+4];
        (*rec)->attribute3 = blockPtr->data[tid+8];
        (*rec)->attribute4 = blockPtr->data[tid+12];
        (*rec)->attribute5 = blockPtr->data[tid+13];
        (*rec)->attribute6 = blockPtr->data[tid+14];
        (*rec)->attribute7 = blockPtr->data[tid+15];
        getmeta[fd]=rid; //Updata meta data
        
        
    }
    
    
    
    
    
    return 0;
}

errCode HFL_get_this_rec(fileDesc fd, recordID rid, record** rec)
{
    fprintf(stderr, "Attempting to get record %d from file %d\n", rid, fd);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    
    block *blockPtr = NULL;
    
    int bid=(rid/256)+1;
    
    int tid=(rid%256)*16;
    
    int i = 0;
    while (i<100) {
        if (bid == bufferpool[i].blockID && fd == bufferpool[i].fd) {
            printf("GTR-find record in bufferpool %d\n",i);
            (*rec)->attribute1 = bufferpool[i].data[tid];
            (*rec)->attribute2 = bufferpool[i].data[tid+4];
            (*rec)->attribute3 = bufferpool[i].data[tid+8];
            (*rec)->attribute4 = bufferpool[i].data[tid+12];
            (*rec)->attribute5 = bufferpool[i].data[tid+13];
            (*rec)->attribute6 = bufferpool[i].data[tid+14];
            (*rec)->attribute7 = bufferpool[i].data[tid+15];
            getmeta[fd]=rid;
            break;

        }
        i++;
    }
    printf("GTR-i equals to %d\n",i);
    
    if (i == 100) {
        printf("GNR-find record in file fd=%d\n",fd);
        BM_get_this_block(fd, bid, &blockPtr);
        (*rec)->attribute1 = blockPtr->data[tid];
        (*rec)->attribute2 = blockPtr->data[tid+4];
        (*rec)->attribute3 = blockPtr->data[tid+8];
        (*rec)->attribute4 = blockPtr->data[tid+12];
        (*rec)->attribute5 = blockPtr->data[tid+13];
        (*rec)->attribute6 = blockPtr->data[tid+14];
        (*rec)->attribute7 = blockPtr->data[tid+15];
        getmeta[fd]=rid; //Updata meta data

    }
    
    
    return 0;
}

scanDesc HFL_open_file_scan(fileDesc fd)
{
    fprintf(stderr, "Opening a file scan for file %d\n", fd);
    int i = 0;
    while (i < 100) {
        if ( sdmeta[i] < 0) {
            printf("Now the ScanDesc %d is used for fd %d\n",i , fd);
            sdmeta[i] = fd;
            break;
        }
        i++;
    }
    int sd=i;
    if (i == 100) {
        printf("All ScanDescs are busy, please close one at first\n");
        return 0;
    }
    return sd;
}



errCode HFL_find_next_rec(scanDesc sd, record** rec)
{
    fprintf(stderr, "scanDesc %d is scanning to the next record\n", sd);
    
    block *blockPtr = NULL;
    
    int fd = sdmeta[sd];
    
    int rid = scanmeta[sd][fd];
    
    int bid = (rid / 256) + 1;
    
    int tid = (rid % 256) * 16;
    
    int i = 0;
    
    while (i < 100 ) {
        if (bid == bufferpool[i].blockID && fd == bufferpool[i].fd) {
            printf("FNR-the record is found in bufferpoor %d\n",i);
            (*rec)->attribute1 = bufferpool[i].data[tid];
            (*rec)->attribute2 = bufferpool[i].data[tid+4];
            (*rec)->attribute3 = bufferpool[i].data[tid+8];
            (*rec)->attribute4 = bufferpool[i].data[tid+12];
            (*rec)->attribute5 = bufferpool[i].data[tid+13];
            (*rec)->attribute6 = bufferpool[i].data[tid+14];
            (*rec)->attribute7 = bufferpool[i].data[tid+15];
            (scanmeta[sd][fd])++;
            break;
            
        }
        i++;
    }
    
    if (i == 100) {
        printf("FNR- Find record in fd %d\n",fd);
        BM_get_this_block(fd, bid, &blockPtr);
        (*rec)->attribute1 = blockPtr->data[tid];
        (*rec)->attribute2 = blockPtr->data[tid+4];
        (*rec)->attribute3 = blockPtr->data[tid+8];
        (*rec)->attribute4 = blockPtr->data[tid+12];
        (*rec)->attribute5 = blockPtr->data[tid+13];
        (*rec)->attribute6 = blockPtr->data[tid+14];
        (*rec)->attribute7 = blockPtr->data[tid+15];
        (scanmeta[sd][fd])++;
        
    }
    
    
    return 0;
}

errCode HFL_close_file_scan(scanDesc sd)
{
    fprintf(stderr, "Closing file scan %d", sd);
    
    
    int fd = sdmeta[sd];
    scanmeta[sd][fd] = 0;
    sdmeta[sd] = -1;
    
    printf("The ScanDesc %d is closed\n",sd);
    
    
    
    return 0;
}

void HFL_print_error(errCode ec)
{
  fprintf(stderr, "Printing error code %d\n", ec);
}




