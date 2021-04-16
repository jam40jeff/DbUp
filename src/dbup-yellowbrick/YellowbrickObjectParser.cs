using DbUp.Support;

namespace DbUp.Yellowbrick
{
    /// <summary>
    /// Parses Sql Objects and performs quoting functions.
    /// </summary>
    public class YellowbrickObjectParser : SqlObjectParser
    {
        public YellowbrickObjectParser() : base("\"", "\"")
        {
        }
    }
}
