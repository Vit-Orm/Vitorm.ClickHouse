using ClickHouse.Client.ADO;

using Vit.Core.Util.ConfigurationManager;

using Vitorm.Sql;

namespace Vitorm.MsTest
{
    [System.ComponentModel.DataAnnotations.Schema.Table("User")]
    public class User
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.Column("userId")]
        public int id { get; set; }
        [System.ComponentModel.DataAnnotations.Schema.Column("userName")]
        public string name { get; set; }
        [System.ComponentModel.DataAnnotations.Schema.Column("userBirth")]
        public DateTime? birth { get; set; }
        [System.ComponentModel.DataAnnotations.Schema.Column("userFatherId")]
        public int? fatherId { get; set; }
        [System.ComponentModel.DataAnnotations.Schema.Column("userMotherId")]
        public int? motherId { get; set; }
        [System.ComponentModel.DataAnnotations.Schema.Column("userClassId")]
        public int? classId { get; set; }
        public static User NewUser(int id, bool forAdd = false) => new User { id = id, name = "testUser" + id };

        public static List<User> NewUsers(int startId, int count = 1, bool forAdd = false)
        {
            return Enumerable.Range(startId, count).Select(id => NewUser(id, forAdd)).ToList();
        }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("UserClass")]
    public class UserClass
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.Column("classId")]
        public int id { get; set; }
        [System.ComponentModel.DataAnnotations.Schema.Column("className")]
        public string name { get; set; }

        public static List<UserClass> NewClasses(int startId, int count = 1)
        {
            return Enumerable.Range(startId, count).Select(id => new UserClass { id = id, name = "class" + id }).ToList();
        }
    }


    public class DataSource
    {
        public static void WaitForUpdate() => Thread.Sleep(1000);

        readonly static string connectionString = Appsettings.json.GetStringByPath("Vitorm.ClickHouse.connectionString");

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
                    InitDbContext(dbContext);

                    initedDefaultIndex = true;
                }
            }

            return dbContext;
        }

        static void InitDbContext(SqlDbContext dbContext)
        {
            #region #1 init User
            {
                dbContext.Drop<User>();
                dbContext.Create<User>();

                var users = new List<User> {
                    new User { id=1,  name="u146", fatherId=4, motherId=6 },
                    new User { id=2,  name="u246", fatherId=4, motherId=6 },
                    new User { id=3,  name="u356", fatherId=5, motherId=6 },
                    new User { id=4,  name="u400" },
                    new User { id=5,  name="u500" },
                    new User { id=6,  name="u600" },
                };

                users.ForEach(user =>
                {
                    user.birth = DateTime.Parse("2021-01-01 00:00:00").AddHours(user.id);
                    user.classId = user.id % 2 + 1;
                });

                dbContext.AddRange(users);
            }
            #endregion

            #region #2 init Class
            {
                dbContext.Drop<UserClass>();
                dbContext.Create<UserClass>();
                dbContext.AddRange(UserClass.NewClasses(1, 6));
            }
            #endregion
        }

    }
}
