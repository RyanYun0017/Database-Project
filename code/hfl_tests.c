#include <assert.h>
#include "projectA.h"
#include "projectB.h"


record mkRecord(int attr1, int attr2, int attr3,
		char attr4, char attr5, char attr6, char attr7);


int main(int argc, char const *argv[])
{

    int errc = 0;

    HFL_init();

    errc = HFL_create_file("testing1.dat");
    if (errc < 0) HFL_print_error(errc);

    fileDesc fd = HFL_open_file("testing1.dat");
    for (int i=0; i<5; i++) {
        BM_alloc_block(fd);
    }
    // The number of records to insert into the file
    int numRecords = 1000;

    // Generate some nonsense records and insert them into the file
    for (int i = 0; i < numRecords; i++) {
        record rec = mkRecord(i % 10, (i + 1) % 10, (i + 2) % 10,
			     'a', 'b', 'c', 'd');
        recordID rid = HFL_insert_rec(fd, &rec);
        printf("rid is %d\n",rid);
    }
    
    
   
    
    
    record* recPtr = NULL;
    
    // Retrieve the first record from the file    
    errc = HFL_get_first_rec(fd, &recPtr);
    
    if (errc < 0) HFL_print_error(errc);
    // Print attributes of the first record
    printf("The first record is %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
    
    
    // Iterate through all records in the file
    for (int i = 0; i < numRecords - 1; i++) {
        errc = HFL_get_next_rec(fd, &recPtr);
        if (errc < 0) HFL_print_error(errc);
        // Print attributes of the next record
        printf("This record is %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
    }
    

    // Retrieve the record wth ID 2
    errc = HFL_get_this_rec(fd, 2, &recPtr);
    if (errc < 0) HFL_print_error(errc);
    // Print attributes of the record with ID 2
     printf("This record with rid=2 is  %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
    
    
    

    // Delete all records with even IDs between 0 and 100
    for (int i = 0; i < 100; i++) {
        if (i % 2 == 0) {
	    errc = HFL_delete_rec(fd, i);
	    if (errc < 0) {
	        HFL_print_error(errc);
	    } else {
	        numRecords--;
	    }
	}
    }

    // Iterate through the records in the file a second time
    // to confirm that some have been deleted
    for (int i= 0 ; i < 100; i++) {
        HFL_get_this_rec(fd, i, &recPtr);
        printf("This record is  %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
        
    }
    
    
    
    errc = HFL_get_first_rec(fd, &recPtr);
    if (errc < 0) HFL_print_error(errc);
    // Print attributes of the first record
    for (int i = 0; i < numRecords - 1; i++) {
        errc = HFL_get_next_rec(fd, &recPtr);
        if (errc < 0) HFL_print_error(errc);
        // Print attributes of the next record
    }

    // Try to retrieve the record with ID 2--it should be missing
    errc = HFL_get_this_rec(fd, 2, &recPtr);
    assert (errc < 0);

    // Close the file
    errc = HFL_close_file(fd);
    if (errc < 0) HFL_print_error(errc);
    
    
    
    
    // The following part is for testing my filescan function (HFL_open_file_scan, HFL_open_file_scan, HFL_find_next_rec)
    
    // Part 1. Test open scan and close scan in one loop
    
    int sd1 = HFL_open_file_scan(fd);
    
    for (int i = 0; i < 1000; i++) {
        HFL_find_next_rec(sd1, &recPtr);
        printf("This record is  %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
        
        
    }
    
    HFL_close_file_scan(sd1);
    
    //Part 2: Open a file scan, and then open another one without close the former one.
    
     sd1 = HFL_open_file_scan(fd);
    
    for (int i = 0; i < 20; i++) {
        HFL_find_next_rec(sd1, &recPtr);
        printf("This record is  %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
        // Find the first 20 records in sd1. And print them.
    }
    
    // Open another scan for the same fd.
    int sd2 = HFL_open_file_scan(fd);
    
    for (int i = 0; i < 5; i ++) {
        HFL_find_next_rec(sd2, &recPtr);
        printf("This record is  %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
        // Find the first 5 records in sd2. Then print.
    }
    
    
    // Then we continue the sd1, we should get the 21st record.
    HFL_find_next_rec(sd1, &recPtr);
    printf("This record is  %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
    
    // Because we use sd2, this should return the 6th record.
    HFL_find_next_rec(sd2, &recPtr);
    printf("This record is  %d%d%d%c%c%c%c\n",recPtr->attribute1,recPtr->attribute2,recPtr->attribute3,recPtr->attribute4,recPtr->attribute5,recPtr->attribute6,recPtr->attribute7);
    

    return 0;
}

/* Utility function for creating a record struct */
record mkRecord(int attr1, int attr2, int attr3,
                char attr4, char attr5, char attr6, char attr7)
{
    record r;
    r.attribute1 = attr1;
    r.attribute2 = attr2;
    r.attribute3 = attr3;
    r.attribute4 = attr4;
    r.attribute5 = attr5;
    r.attribute6 = attr6;
    r.attribute7 = attr7;
    return r;
}
