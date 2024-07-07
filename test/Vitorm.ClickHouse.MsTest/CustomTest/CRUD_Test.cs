using Microsoft.VisualStudio.TestTools.UnitTesting;

using System.Data;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public partial class CRUD_Test
    {
        static DbContext CreateDbContext() => DataSource.CreateDbContextForWriting();



        #region #1 Create

        [TestMethod]
        public void Test_Create()
        {
            using var dbContext = CreateDbContext();

            var newUserList = User.NewUsers(7, 4, forAdd: true);


            // #1 Add
            dbContext.Add(newUserList[0]);

            // #2 AddRange
            dbContext.AddRange(newUserList.Skip(1));

            DataSource.WaitForUpdate();

            // assert
            {
                var userList = dbContext.Query<User>().Where(user => user.id >= 7).ToList();
                Assert.AreEqual(newUserList.Count, userList.Count());
                Assert.AreEqual(0, userList.Select(m => m.id).Except(newUserList.Select(m => m.id)).Count());
                Assert.AreEqual(0, userList.Select(m => m.name).Except(newUserList.Select(m => m.name)).Count());
            }

            try
            {
                dbContext.Add(newUserList[0]);
            }
            catch (Exception ex)
            {
                Assert.Fail("should be able to add same key twice");
            }


        }
        #endregion




        #region #4 Delete


        [TestMethod]
        public void Test_Delete()
        {
            using var dbContext = CreateDbContext();

            // #1 Delete
            {
                dbContext.Delete(User.NewUser(1));
            }

            // #2 DeleteRange
            {
                dbContext.DeleteRange(User.NewUsers(2, 2));

            }

            // #3 DeleteByKey
            {
                var user = User.NewUser(4);
                var key = dbContext.GetEntityDescriptor(typeof(User)).key;
                var keyValue = key.GetValue(user);
                dbContext.DeleteByKey<User>(keyValue);

            }

            // #4 DeleteByKeys
            {
                var users = User.NewUsers(5, 2);
                var key = dbContext.GetEntityDescriptor(typeof(User)).key;
                var keyValues = users.Select(user => key.GetValue(user));
                dbContext.DeleteByKeys<User, object>(keyValues);
            }

            DataSource.WaitForUpdate();

            // assert
            {
                var userList = dbContext.Query<User>().ToList();
                Assert.AreEqual(0, userList.Count());
            }
        }
        #endregion


    }
}
