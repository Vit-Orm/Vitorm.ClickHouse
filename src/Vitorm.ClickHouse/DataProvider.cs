﻿using System.Collections.Generic;

using Vitorm.Sql;
using Vitorm.Sql.DataProvider;

namespace Vitorm.ClickHouse
{
    public class DataProvider : SqlDataProvider
    {
        protected Dictionary<string, object> config;
        protected DbConfig dbConfig;

        public override void Init(Dictionary<string, object> config)
        {
            this.config = config;
            this.dbConfig = new(config);
        }

        public override SqlDbContext CreateDbContext() => new SqlDbContext().UseClickHouse(dbConfig);
    }
}
