using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using OfficeOpenXml;

namespace SQL2Excel
{
    public class Sql2Excel
    {
        private readonly ISqlRepository _sqlRepository;
        private readonly Dictionary<Query, DataTable> _results;

        public Sql2Excel(ISqlRepository sqlRepository)
        {
            _sqlRepository = sqlRepository;
            _results = new Dictionary<Query, DataTable>();
            Queries = new List<Query>();
        }

        public Sql2Excel() : this(new SqlRepository())
        {
        }

        private List<Query> Queries { get; }

        public Sql2Excel AddQuery(Query query)
        {
            Queries.Add(query);
            return this;
        }
        
        public Sql2Excel Execute()
        {
            foreach (var query in Queries)
            {
                _results.Add(query, _sqlRepository.WithSqlStatement(query.CommandText).Execute().Tables[0]);
            }

            return this;
        }

        public void Save(string filename, bool overwrite = true)
        {
            if (!_results.Any())
                this.Execute();

            if (File.Exists(filename))
            {
                if (overwrite)
                    File.Delete(filename);
                else
                    throw new ApplicationException(string.Format("{0} already exists", filename));
            }

            var newFile = new FileInfo(filename);
            using (var xlPackage = new ExcelPackage(newFile))
            {
                foreach (var result in _results)
                {
                    var sheetName = result.Key.Name.Length > 31 ? result.Key.Name.Substring(0, 31) : result.Key.Name;
                    var worksheet = xlPackage.Workbook.Worksheets.Add(sheetName);

                    worksheet.Cells["A1"].LoadFromDataTable(result.Value, true);
                }

                xlPackage.Save();
            }
        }

        public void Save(Stream stream)
        {            
        }
    }
}