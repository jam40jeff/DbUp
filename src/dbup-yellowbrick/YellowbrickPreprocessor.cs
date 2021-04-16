using DbUp.Engine;

namespace DbUp.Yellowbrick
{
    /// <summary>
    /// This preprocessor makes adjustments to your sql to make it compatible with Yellowbrick.
    /// </summary>
    public class YellowbrickPreprocessor : IScriptPreprocessor
    {
        /// <summary>
        /// Performs some preprocessing step on a Yellowbrick script.
        /// </summary>
        public string Process(string contents) => contents;
    }
}
