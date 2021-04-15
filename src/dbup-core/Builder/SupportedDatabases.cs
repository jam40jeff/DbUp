namespace DbUp.Builder
{
    /// <summary>
    /// Add extension methods to this type if you plan to add support for additional databases.
    /// </summary>
    public class SupportedDatabases
    {
        public static readonly SupportedDatabases Instance = new SupportedDatabases();

        private SupportedDatabases()
        {
        }
    }
}
