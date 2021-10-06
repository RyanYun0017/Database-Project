#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <memory.h>
#include "projectA.h"

block bufferpool[100];
int COUNT=0;
int lastuse=0;


/*
 * This is a dummy implementation to make sure that everything compiles.
 * You should expand this.
 * */

void init_block(block** bptr)
{
	*bptr = malloc(sizeof(block));
	(*bptr)->data = malloc(FRAMESIZE);
	(*bptr)->dirty=0;
	(*bptr)->pinCount=0;
}


void populate_block(block* blockPtr);

void BM_init(char* newfile){
    
    for (int i=0; i<100; i++) {
        block* p=&bufferpool[i];
        init_block(&p);
    }

    
}


//Create an empty file. And write an empty metadata into the first FRAMESIZE size.
errCode BM_create_file( const char *filename ) {
	fprintf(stderr, "Calling BM_create_file with name: %s\n", filename);
    FILE *fc;
    fc=fopen(filename, "w+");
    if (fc==NULL) {
        return 1;
    }
    char meta[4096]="";
    fseek(fc, 0, SEEK_SET);
    fwrite(meta, FRAMESIZE, 1, fc);
    fclose(fc);
	return 0;
}

fileDesc BM_open_file( const char *filename ) {
	fprintf(stderr, "Calling BM_open_file with name: %s\n", filename);
    FILE *fo;
    int fd;
    fo=fopen(filename, "rb+");
    fd=fileno(fo);
    
	return fd;
}

errCode BM_close_file( fileDesc fd ) {
	fprintf(stderr, "Attempting to close file: %d\n", fd);
    FILE *fo;
    int a;
    fo = fdopen(fd,"rb+");
    a=fclose(fo);
    if (a==0) {
        printf("File has been closed successfully\n");
        return 0;
    }
    else {
        printf("Fail to close the file");
        return 1;
    }

}

errCode BM_get_first_block( fileDesc fd, block** blockPtr ) {
	fprintf(stderr, "Attempting to read first block from file %d\n", fd);
    // Initialize
    block* tempbp=NULL;
    init_block(&tempbp);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    
    //Read the data from the file. Including check the flag, which indicates the block is disposed or not.
    int bid=1;
    char* temp=malloc(FRAMESIZE);
    fseek(fp, FRAMESIZE, SEEK_SET);
    while(1){
        char flag[1];
        fread(flag, sizeof(char), 1, fp);
        fread(temp, FRAMESIZE, 1, fp);
        if (flag-'0' == 1) {
            bid++;
            continue;
        }
        else{
            printf("The block id of first block is %d\n",bid);
            break;
        }
        
    }
    rewind(fp);
    
    
    //Update the metadata into the file.
    updateMeta(fp, bid);

    
    
    
    
    tempbp->data = temp;
    tempbp->fd=fd;
    tempbp->blockID=bid;
    tempbp->pinCount++;
    
    //Check if the buffer pool is full. If full, unpin the last used one. If not, wirte the data in it.
    if (COUNT<100) {
        for (int i=0; i<100; i++) {
            
            if (bufferpool[i].blockID==0) {
                bufferpool[i]=*tempbp;
                lastuse=i;
                COUNT++;
                break;
            }
        }
    }
    else{
        BM_unpin_block(&bufferpool[lastuse]);
        bufferpool[lastuse]=*tempbp;
    }
    
    
    
    for (int i=0; i<100; i++) {
        if (fd==bufferpool[i].fd && bid==bufferpool[i].blockID) {
            bufferpool[i].pinCount++;
            *blockPtr= &bufferpool[i];
            lastuse=i;
            
        }
    }
    //fclose(fp);
    
    return 0;
    
    
    
    
    

}


errCode BM_get_next_block( fileDesc fd, block** blockPtr ) {
	fprintf(stderr, "Attempting to get next block from file %d\n", fd);
	/*block* new_pointer = NULL;
	init_block(&new_pointer);
	new_pointer->pinCount++;
	*blockPtr = new_pointer;*/
    
    //Initialize.
    char* temp=malloc(FRAMESIZE);
    block* tempbp;
    init_block(&tempbp);
    FILE* fp=fdopen(fd, "rb+");
    int bid = read_META(fp);
    bid=bid+1;
    
    //Set the file pointer on the block we want get. And read the data into a temp char array.
    
    fseek(fp, FRAMESIZE+(FRAMESIZE+sizeof(char))*(bid-1), SEEK_CUR);
    while(1){
        char flag[1];
        fread(flag, sizeof(char), 1, fp);
        fread(temp, FRAMESIZE, 1, fp);
        if (flag-'0' == 1) {
            bid++;
            continue;
            
        }
        else{
            printf("The blockID of this block is %d\n",bid);
            break;
        }
        
    }
    rewind(fp);

    
    
    //Update the metadata into the file.
    
    updateMeta(fp, bid);
    tempbp->data = temp;
    tempbp->fd=fd;
    tempbp->blockID=bid;
    tempbp->pinCount++;
    
    //Check if the buffer pool is full. If full, unpin the last used one. If not, wirte the data in it.
    if (COUNT<100) {
        for (int i=0; i<100; i++) {
            if (bufferpool[i].blockID==0) {
                bufferpool[i]=*tempbp;
                lastuse=i;
                COUNT++;
                break;
            }
        }
    }
    else{
        BM_unpin_block(&bufferpool[lastuse]);
        bufferpool[lastuse]=*tempbp;
    }
    
    for (int i=0; i<100; i++) {
        if (fd==bufferpool[i].fd && bid==bufferpool[i].blockID) {
            bufferpool[i].pinCount++;
            *blockPtr= &bufferpool[i];
            lastuse=i;
            
        }
    }
    //fclose(fp);
    
    return 0;
    

}

errCode BM_get_this_block( fileDesc fd, int blockID, block** blockPtr ) {
    fprintf(stderr, "Attempting to get block %d from file %d\n", blockID, fd);
    /*block* new_pointer = NULL;
     init_block(&new_pointer);
     new_pointer->pinCount++;
     
     populate_block(new_pointer);
     *blockPtr = new_pointer;*/
    
    // Initialize
    
    blockID=blockID;
    block* tempbp;
    init_block(&tempbp);
    FILE* fp;
    fp=fdopen(fd, "rb+");
    char* temp=malloc(FRAMESIZE);
    int bid=blockID;
    
    
    //Set the file pointer on the block we want to get. And read data into a temp char array
    fseek(fp, FRAMESIZE+(FRAMESIZE+sizeof(char))*(bid-1), SEEK_CUR);
    while(1){
        char flag[1];
        fread(flag, sizeof(char), 1, fp);
        fread(temp, FRAMESIZE, 1, fp);
        if (flag-'0' == 1) {
            bid++;
            continue;
            
        }
        else{
            printf("The blockID of this block is %d\n",bid);
            break;
        }
              
    }
    rewind(fp);
    
    //Update the metadata into the file.
    
    updateMeta(fp, bid);
    
    tempbp->data = temp;
    tempbp->fd=fd;
    tempbp->blockID=bid;
    tempbp->pinCount++;
    
    //Check if the buffer pool is full. If full, unpin the last used one. If not, wirte the data in it.
    if (COUNT<100) {
        for (int i=0; i<100; i++) {
            if (bufferpool[i].blockID==0) {
                bufferpool[i]=*tempbp;
                lastuse=i;
                COUNT++;
                break;
            }
        }
    }
    else{
        BM_unpin_block(&bufferpool[lastuse]);
        bufferpool[lastuse]=*tempbp;
    }
    
    for (int i=0; i<100; i++) {
        if (fd==bufferpool[i].fd && bid==bufferpool[i].blockID) {
            bufferpool[i].pinCount++;
            *blockPtr= &bufferpool[i];
            lastuse=i;
            
        }
    }

    
    //fclose(fp);
    
    
    return 0;

}







errCode BM_alloc_block( fileDesc fd ) {
	fprintf(stderr, "Attempting to allocate block in file %d\n", fd);
    FILE *fp;
    fp=fdopen(fd, "rb+");
    char str[FRAMESIZE+sizeof(char)]="";
    fseek(fp, 0, SEEK_END);
    //Allocate the data size space into file. Because we have a flag in front of the data, the data size FRAMESIZE+sizeof(char);
    fwrite(str,FRAMESIZE+sizeof(char), 1, fp);
    rewind(fp);
    //fclose(fp);
    
    
	return 0;
}

errCode BM_dispose_block( fileDesc fd, int blockID ) {
	fprintf(stderr, "Attempting to dispose of block %d from file %d\n",blockID, fd);
    FILE* fp=fdopen(fd, "rb+");
    int bid = blockID;
    
    //Find the flag we wanna change according to the BlockID.
    fseek(fp, (FRAMESIZE+sizeof(char))*(bid-1)+FRAMESIZE, SEEK_SET);
    char flag[1]={'1'};
    fwrite(flag, sizeof(char), 1, fp);
    //fclose(fp);
    
	return 0;
}

errCode BM_unpin_block( block* blockPtr ) {
    
	fprintf(stderr, "Unpinning block\n");
    
    int bid=blockPtr->blockID;
    int fd = blockPtr->fd;
    FILE* fp=fdopen(fd, "wb+");
    
    
    fseek(fp,(sizeof(char)+FRAMESIZE)*bid, SEEK_SET);
    fwrite(blockPtr->data, 1, FRAMESIZE+sizeof(char), fp);
    rewind(fp);
    block* bp=NULL;
    init_block(&bp);
    bp->data=NULL;
    bp->blockID=0;
    bp->dirty=0;
    bp->fd=0;
    bp->pinCount=0;
    
    for (int i=0; i<100; i++) {
        if (bid==bufferpool[i].blockID && fd==bufferpool[i].fd) {
            bufferpool[i]=*bp;
        }
        
    }
    blockPtr->data=NULL;
    blockPtr->blockID=0;
    blockPtr->dirty=0;
    blockPtr->fd=0;
    blockPtr->pinCount=0;
    
    
   
    COUNT--;
    //fclose(fp);
    
    
	
	return 0;
}

void BM_print_error( errCode ec ) {
	fprintf(stderr, "Printing error code %d\n", ec);
}




// Read the metadata of the file according to the file poiinter.
int read_META(FILE*fp){
    char *temp=malloc(FRAMESIZE);
    fseek(fp, 0, SEEK_SET);
    fread(temp, 1, FRAMESIZE, fp);
    int metadata;
    sscanf(temp, "%d",&metadata);
    printf("The metadata is %d\n",metadata);
    rewind(fp);
    //fclose(fp);
    return metadata;
}



//Update the metadata according to the file pointer and the BlockID.
int updateMeta(FILE*fp,int bid ){
    rewind(fp);
    char* metadata=malloc(FRAMESIZE);
    sprintf(metadata,"%d",bid);
    printf("metadata is: %s\n",metadata);
    fwrite(metadata, FRAMESIZE, 1, fp);
    rewind(fp);
    //fclose(fp);
    return 0;
}




