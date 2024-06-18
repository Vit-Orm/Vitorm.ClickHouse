using Vitorm.Sql;
using Vit.Extensions;
using Vit.Core.Util.ConfigurationManager;
using System.ComponentModel.DataAnnotations.Schema;
using ClickHouse.Client.ADO;

namespace Vitorm.MsTest
{
    [System.ComponentModel.DataAnnotations.Schema.Table("User")]
    public class User
    {
        [System.ComponentModel.DataAnnotations.Key]
        public int id { get; set; }
        public string name { get; set; }
        public DateTime? birth { get; set; }

        public int? fatherId { get; set; }
        public int? motherId { get; set; }
        public static User NewUser(int id) => new User { id = id, name = "testUser" + id };

        public static List<User> NewUsers(int startId, int count = 1)
        {
            return Enumerable.Range(startId, count).Select(NewUser).ToList();
        }
    }


    public class DataSource
    {
        readonly static string connectionString = Appsettings.json.GetStringByPath("App.Db.ConnectionString");

        static int dbIndexCount = 0;
        public static SqlDbContext CreateDbContextForWriting()
        {
            dbIndexCount++;
            var dbName = "dev-orm" + dbIndexCount;
            var connectionString = DataSource.connectionString;

            // #1 create db
            {
                var dbContext = new SqlDbContext();
                dbContext.UseClickHouse(connectionString);
                dbContext.Execute(sql: $"create database if not exists `{dbName}`; ");
            }

            // #2
            {
                ClickHouseConnectionStringBuilder builder = new ClickHouseConnectionStringBuilder(connectionString);
                builder.Database = dbName;
                connectionString = builder.ToString();

                var dbContext = new SqlDbContext();
                dbContext.UseClickHouse(connectionString);

                dbContext.Execute(sql: "DROP TABLE if exists `User`;");
                dbContext.Create<User>();
                InitDbContext(dbContext);

                return dbContext;
            }
        }

        static bool initedDefaultIndex = false;
        public static SqlDbContext CreateDbContext()
        {
            var dbContext = new SqlDbContext();
            dbContext.UseClickHouse(connectionString);

            lock (typeof(DataSource))
            {
                if (!initedDefaultIndex)
                {
                    //dbContext.Execute(sql: "truncate TABLE `User`;");

                    dbContext.Execute(sql: "DROP TABLE if exists `User`;");
                    dbContext.Create<User>();
                    InitDbContext(dbContext);

                    initedDefaultIndex = true;
                }
            }

            return dbContext;
        }

        static void InitDbContext(SqlDbContext dbContext)
        {
            var users = new List<User> {
                    new User { id=1,  name="u146", fatherId=4, motherId=6 },
                    new User { id=2,  name="u246", fatherId=4, motherId=6 },
                    new User { id=3,  name="u356", fatherId=5, motherId=6 },
                    new User { id=4,  name="u400" },
                    new User { id=5,  name="u500" },
                    new User { id=6,  name="u600" },
                };

            users.ForEach(user => { user.birth = DateTime.Parse("2021-01-01 00:00:00").AddHours(user.id); });

            dbContext.AddRange(users);
        }

    }
}
