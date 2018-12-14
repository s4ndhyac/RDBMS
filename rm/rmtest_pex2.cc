#include "rm_test_util.h"

RC RM_TEST_PRIVATE_EXTRA_2(const string &tableName)
{
    // Functions Tested
    // 1. Add Attribute **
    // 2. Insert Tuple
    cout << endl << "***** In RM Private Extra Credit Test Case 2 *****" << endl;

    RID rid;
    RID rid2;
    int tupleSize=0;
    int tupleSize2=0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);
    void *tuple2 = malloc(200);
    void *returnedData2 = malloc(200);

	int tweetid = 10;
	int userid = 12;
	float sender_location = 98.99;
	float send_time = 10.11;
	string referred_topics = "private2";
	int referred_topics_length = 8;
	string message_text = "PassOrFail2";
	int message_text_length = 11;		
	string status_msg = "testcasewillpass";
	int status_msg_length = 16;		

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    prepareTweetTuple(attrs.size(), nullsIndicator, tweetid, userid, sender_location, send_time, referred_topics_length, referred_topics, message_text_length, message_text, tuple, &tupleSize);
    rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Test Add Attribute
    Attribute attr;
    attr.name = "status_msg";
    attr.type = TypeVarChar;
    attr.length = 30;
    rc = rm->addAttribute(tableName, attr);
    assert(rc == success && "RelationManager::addAttribute() should not fail.");

    // GetAttributes
    vector<Attribute> newAttrs;
    rc = rm->getAttributes(tableName, newAttrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    // Test Insert Tuple
    prepareTweetTupleAfterAdd(newAttrs.size(), nullsIndicator, tweetid, userid, sender_location, send_time, referred_topics_length, referred_topics, message_text_length, message_text, status_msg_length, status_msg, tuple2, &tupleSize2);
    rc = rm->insertTuple(tableName, tuple2, rid2);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Test Read Tuple - old tuple - status_msg field should be NULL.
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");
	
    // Test Read Tuple - new tuple
    rc = rm->readTuple(tableName, rid2, returnedData2);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    cout << "Inserted Data before addAttribute():" << endl;
    rc = rm->printTuple(attrs, tuple);

    cout << "Returned Data before addAttribute():" << endl << endl;
    rc = rm->printTuple(newAttrs, returnedData);

    cout << "Inserted Data after addAttribute():" << endl;
    rc = rm->printTuple(newAttrs, tuple2);

    cout << "Returned Data after addAttribute():" << endl;
    rc = rm->printTuple(newAttrs, returnedData2);

    rc = rm->deleteTable(tableName);

    if (memcmp(returnedData2, tuple2, tupleSize2) != 0)
    {
        cout << "***** [FAIL] RM Private Extra Credit Test Case 2 Failed *****" << endl << endl;
        free(tuple);
        free(returnedData);
        free(tuple2);
        free(returnedData2);
        return -1;
    }
    else
    {
        cout << "***** RM Private Extra Credit Test Case 2 Finished. The result will be examined. *****" << endl << endl;
        free(tuple);
        free(returnedData);
        free(tuple2);
        free(returnedData2);
        return success;
    }

}

int main()
{
    // Add Attributes
    createTweetTable("tbl_private_extra_2");
    RC rcmain = RM_TEST_PRIVATE_EXTRA_2("tbl_private_extra_2");

    return rcmain;
}
