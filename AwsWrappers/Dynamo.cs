using System.Collections.Generic;
using System.Diagnostics;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

using Newtonsoft.Json;

namespace AwsDal
{
    public class Dynamo
    {
        // notes at: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DotNetSDKMidLevel.html#DeleteItemMidLevelDotNet

        private IAmazonDynamoDB _Client;

        public Dynamo(string awsAcesskeyId, string awsSecretAccessKey)
        {
            var config = new AmazonDynamoDBConfig();
            config.RegionEndpoint = RegionEndpoint.USWest1;
            _Client = new AmazonDynamoDBClient(awsAcesskeyId, awsSecretAccessKey, config);
        }

        public List<string> GetTablesList()
        {
            var listTablesRequest = new ListTablesRequest();
            var listTablesResponse = _Client.ListTables(listTablesRequest);
            return listTablesResponse.TableNames;
        }

        public void GetTablesDetails()
        {
            List<string> tables = GetTablesList();

            foreach (string table in tables)
            {
                DescribeTableRequest describeTableRequest = new DescribeTableRequest(table);
                DescribeTableResponse describeTableResponse = _Client.DescribeTable(describeTableRequest);
                TableDescription tableDescription = describeTableResponse.Table;
                Debug.WriteLine(string.Format("Printing information about table {0}:", tableDescription.TableName));
                Debug.WriteLine(string.Format("Created at: {0}", tableDescription.CreationDateTime));
                List<KeySchemaElement> keySchemaElements = tableDescription.KeySchema;

                foreach (KeySchemaElement schema in keySchemaElements)
                {
                    Debug.WriteLine(string.Format("Key name: {0}, key type: {1}", schema.AttributeName, schema.KeyType));
                }

                Debug.WriteLine(string.Format("Item count: {0}", tableDescription.ItemCount));
                ProvisionedThroughputDescription throughput = tableDescription.ProvisionedThroughput;
                Debug.WriteLine(string.Format("Read capacity: {0}", throughput.ReadCapacityUnits));
                Debug.WriteLine(string.Format("Write capacity: {0}", throughput.WriteCapacityUnits));
                List<AttributeDefinition> tableAttributes = tableDescription.AttributeDefinitions;

                foreach (AttributeDefinition attDefinition in tableAttributes)
                {
                    Debug.WriteLine(string.Format("Table attribute name: {0}", attDefinition.AttributeName));
                    Debug.WriteLine(string.Format("Table attribute type: {0}", attDefinition.AttributeType));
                }

                Debug.WriteLine(string.Format("Table size: {0}b", tableDescription.TableSizeBytes));
                Debug.WriteLine(string.Format("Table status: {0}", tableDescription.TableStatus));
                Debug.WriteLine("====================================================");
            }
        }

        public string GetItemByStringKey(string tableName, string key)
        {
            var table = Table.LoadTable(_Client, tableName);
            var item = table.GetItem(key);
            return item.ToJson();
        }

        /// <summary>
        /// Insert update an item to the given dyanmo table
        /// </summary>
        public void PutItem(string tableName, object input)
        {
            var table = Table.LoadTable(_Client, tableName);

            string json = JsonConvert.SerializeObject(input, new Newtonsoft.Json.Converters.StringEnumConverter());
            var item = Document.FromJson(json);

            var document = table.PutItem(item);
        }

        public void DeleteItemByStringId(string tableName, string key)
        {
            var table = Table.LoadTable(_Client, tableName);
            var document = table.DeleteItem(key);
        }

        public void BatchPutItems(string tableName, IEnumerable<object> input)
        {
            var table = Table.LoadTable(_Client, tableName);
            var batchWriter = table.CreateBatchWrite();

            foreach (var item in input)
            {
                string json = JsonConvert.SerializeObject(input, new Newtonsoft.Json.Converters.StringEnumConverter());
                var buffer = Document.FromJson(json);
                batchWriter.AddDocumentToPut(buffer);
            }

            batchWriter.Execute();
        }

        public void BatchDeleteItems(string tableName, IEnumerable<string> keys)
        {
            var table = Table.LoadTable(_Client, tableName);
            var batchWriter = table.CreateBatchWrite();

            foreach (var key in keys)
                batchWriter.AddKeyToDelete(key);

            batchWriter.Execute();
        }

        public List<string> BatchGetItems(string tableName, IEnumerable<string> keys)
        {
            var table = Table.LoadTable(_Client, tableName);
            var batchWriter = table.CreateBatchGet();
            var output = new List<string>();

            foreach (var key in keys)
                batchWriter.AddKey(key);

            batchWriter.Execute();

            foreach (var item in batchWriter.Results)
            {
                output.Add(item.ToJson());
            }

            return output;
        }
    }
}