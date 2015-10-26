using NUnit.Framework;

namespace SQL2Excel.Tests
{
    [TestFixture]
    public class Sql2ExcelTests
    {
        [TestFixture]
        public class TheAddQueryMethod : Sql2ExcelTests
        {
            [Test]
            public void Test()
            {
                var sql2Excel = new Sql2Excel();

                sql2Excel
                    .AddQuery(new Query {Name = "Import", CommandText = "SELECT * FROM Import"})
                    .AddQuery(new Query {Name = "ImportLines", CommandText = "SELECT * FROM ImportLine"})                    
                    //.Execute()
                    .Save(@"c:\temp\queryoutput.xlsx", true);
            }        
        }
    }
}