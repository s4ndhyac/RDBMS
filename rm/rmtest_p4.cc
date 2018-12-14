#include "rm_test_util.h"


int TEST_RM_PRIVATE_4(const string &tableName)
{
    // Functions tested
    // 1. Scan Table (VarChar)
    cout << endl << "***** In RM Test Case Private 4 *****" << endl;

    RID rid; 
    vector<string> attributes;   
    void *returnedData = malloc(300);

    void *value = malloc(16);
    string msg = "Txt00099";
    int msgLength = 8;
    
    memcpy((char *)value, &msgLength, 4);
    memcpy((char *)value + 4, msg.c_str(), msgLength);
    
    string attr = "message_text";   
    attributes.push_back("sender_location");
    attributes.push_back("send_time");

    RM_ScanIterator rmsi2;
    RC rc = rm->scan(tableName, attr, LT_OP, value, attributes, rmsi2);
    if(rc != success) {
        free(returnedData);
	    cout << "***** RM Test Case Private 4 failed. *****" << endl << endl;
        return -1;
    }
    
    float sender_location = 0.0;
    float send_time = 0.0;
    
    int counter = 0;
    while(rmsi2.getNextTuple(rid, returnedData) != RM_EOF)
    {
        counter++;
    
    	sender_location = *(float *)((char *)returnedData + 1);
    	send_time = *(float *)((char *)returnedData + 5);
    	    
        if (!(sender_location >= 0.0 || sender_location <= 98.0 || send_time >= 2000.0 || send_time <= 2098.0))
        {
             cout << "***** A wrong entry was returned. RM Test Case Private 4 failed *****" << endl << endl;
             rmsi2.close();
             free(returnedData);
             free(value);
             return -1;
        }
    }

    rmsi2.close();
    free(returnedData);
    free(value);
    
    if (counter != 99){
       cout << "***** The number of returned tuple: " << counter << " is not correct. RM Test Case Private 4 failed *****" << endl << endl;
    } else {
        cout << "***** RM Test Case Private 4 finished. The result will be examined. *****" << endl << endl;
    }
    return 0;
}

int main()
{
    RC rcmain = TEST_RM_PRIVATE_4("tbl_private_1");
    return rcmain;
}
