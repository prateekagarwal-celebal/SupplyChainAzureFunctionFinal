using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System.Data;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Data.SqlClient;

namespace CreateSchemaDWH
{
    public static class Function1
    {
        //blob connection string
        static string BlobconnectionString = "";
        //Data ware house connection string
        //"DefaultEndpointsProtocol=https;AccountName=marketplacestorage12;AccountKey=y0pWqAUrE5tzH2OlliO5vUMcvAVWPzZ1iy+A8W91M+y/ttQfQoAvwzCM6D8auhienfxTAKoSbxCNpr4efag9Ew==;EndpointSuffix=core.windows.net"
        static string DWHconnectionString = "";
        //"Server=tcp:marketplace-server.database.windows.net,1433;Initial Catalog=marketplace-db;Persist Security Info=False;User ID=celebal;Password=password@123;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"

        [FunctionName("createschemaindwh")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");
            dynamic data = await req.Content.ReadAsAsync<object>();
            JObject token = JsonConvert.DeserializeObject<JObject>(data.ToString());
            BlobconnectionString = (string)token["blob_con_string"];
            DWHconnectionString = (string)token["sql_con_string"];
            if (token != null)
            {
                DateTime aDate = DateTime.Now;
                //Get csv file data
                var csvData = GetCSVBlobData((string)token["filename"], (string)token["containerName"]);
                CreateSchemaAtDWH(csvData, (string)token["tablename"]);
                ////upload csv file on blob
                //string csvfilename = (string)token["filename"];
                //string[] file = csvfilename.Split('.');
                //writtoCSVfile(file[0] + "-" + aDate.ToString("MM/dd/yyyy h:mm tt") + ".csv", (string)token["containerName"], excelData);
            }

            return req.CreateResponse(HttpStatusCode.OK, "File Converted successfully!!");
        }



        /// <summary>
        /// GetExcelBlobData
        ///
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="connectionString"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
        private static string GetCSVBlobData(string filename, string containerName)
        {
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(BlobconnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Retrieve reference to a blob named "test.xlsx"
            CloudBlockBlob blockBlobReference = container.GetBlockBlobReference(filename);

            string csvdata = string.Empty;
            using (var memoryStream = new MemoryStream())
            {
                blockBlobReference.DownloadToStream(memoryStream);
                csvdata = System.Text.Encoding.UTF8.GetString(memoryStream.ToArray());
                csvdata = csvdata.Split(new[] { '\r', '\n' }).FirstOrDefault();

            }

            return csvdata;
        }


        /// <summary>
        /// Create Schema in Data ware house
        /// </summary>
        /// <param name="listofcolumn"></param>
        private static void CreateSchemaAtDWH(string listofcolumn, string _tablename)
        {
            StringBuilder sbSQL_Create_command = new StringBuilder();
            //string command = string.Empty;//create dynamically
            string[] _splitcolumns = listofcolumn.Split(',');
            string Col_datatype_size = "NVARCHAR(3000)";
            sbSQL_Create_command.Append("CREATE TABLE " + _tablename + "(");
            for (int i = 0; i < _splitcolumns.Length; i++)
            {
                sbSQL_Create_command.Append(_splitcolumns[i] + " " + Col_datatype_size + ",");
            }
            sbSQL_Create_command.Remove(sbSQL_Create_command.Length - 1, 1);
            sbSQL_Create_command.Append(")");
            SqlConnection connection = new SqlConnection(DWHconnectionString);
            connection.Open();

            using (SqlCommand cmd = new SqlCommand(sbSQL_Create_command.ToString(), connection))
            {
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// convert data in  csv format
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        private static string Converttocsvdata(DataSet result)
        {
            string csvData = "";
            int row_no = 0;

            while (row_no < result.Tables[0].Rows.Count) // ind is the index of table
                                                         // (sheet name) which you want to convert to csv
            {
                for (int i = 0; i < result.Tables[0].Columns.Count; i++)
                {
                    csvData += result.Tables[0].Rows[row_no][i].ToString() + "\t";
                }
                row_no++;
                csvData += "\n";
            }
            return csvData;
        }
    }
}
