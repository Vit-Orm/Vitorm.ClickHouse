using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Orm_Extensions_ExecuteDelete_Test
    {
        static DbContext CreateDbContext() => DataSource.CreateDbContextForWriting();


        [TestMethod]
        public void Test_ExecuteDelete()
        {
            if (1 == 1)
            {
                using var dbContext = CreateDbContext();
                var userQuery = dbContext.Query<User>();

                userQuery.Where(m => m.id == 2 || m.id == 4).ExecuteDelete();

                Thread.Sleep(1000);

                var newUsers = userQuery.ToList();
                Assert.AreEqual(4, newUsers.Count());
            }

            if (1 == 1)
            {
                using var dbContext = CreateDbContext();
                var userQuery = dbContext.Query<User>();

                var query = from user in userQuery
                            from father in userQuery.Where(father => user.fatherId == father.id).DefaultIfEmpty()
                            where user.id <= 5 && father.name != null
                            select new
                            {
                                user,
                                father
                            };

                query.ExecuteDelete();


                Thread.Sleep(1000);

                var newUsers = userQuery.ToList();
                Assert.AreEqual(3, newUsers.Count());
            }


        }
    }
}
