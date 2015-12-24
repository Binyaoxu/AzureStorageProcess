using System;

namespace StorageProcess.Logger
{
    /// <summary>
    /// Log Level
    /// </summary>
    public enum ServerLogLevel
    {
        Debug = 4,
        Info = 3,
        Warn = 2,
        Error = 1,
        None = 0
    }

    /// <summary>
    /// Log Class
    /// </summary>
    public static class Log
    {
        public static ServerLogLevel LogLevel = ServerLogLevel.Debug;

        /// <summary>
        /// Info Log
        /// </summary>
        /// <param name="text">text</param>
        /// <param name="args">args</param>
        public static void Info(string text, params object[] args)
        {
            if (LogLevel < ServerLogLevel.Info)
                return;

            Conlog("(I) ", text, args);
        }

        /// <summary>
        /// Warn Log
        /// </summary>
        /// <param name="text">text</param>
        /// <param name="args">args</param>
        public static void Warn(string text, params object[] args)
        {
            if (LogLevel < ServerLogLevel.Warn)
                return;
            Console.ForegroundColor = ConsoleColor.DarkRed;
            Conlog("(W) ", text, args);
        }

        /// <summary>
        /// Error Log
        /// </summary>
        /// <param name="ex">exception</param>
        public static void Error(Exception ex)
        {
            if (LogLevel < ServerLogLevel.Error)
                return;
            Console.ForegroundColor = ConsoleColor.Red;
            Conlog("(E) ", ex.ToString());
        }

        /// <summary>
        /// Error Log
        /// </summary>
        /// <param name="text">text</param>
        /// <param name="args">args</param>
        public static void Error(string text, params object[] args)
        {
            if (LogLevel < ServerLogLevel.Error)
                return;
            Console.ForegroundColor = ConsoleColor.Red;
            Conlog("(E) ", text, args);
        }

        /// <summary>
        /// Error Log
        /// </summary>
        /// <param name="ex">eception</param>
        /// <param name="text">text</param>
        /// <param name="args">args</param>
        public static void Error(Exception ex, string text, params object[] args)
        {

            if (LogLevel < ServerLogLevel.Error)
                return;
            Console.ForegroundColor = ConsoleColor.Red;
            Conlog("(E) ", text, args);
        }

        /// <summary>
        /// Debug Log
        /// </summary>
        /// <param name="text">text</param>
        /// <param name="args">args</param>
        public static void Debug(string text, params object[] args)
        {
            if (LogLevel < ServerLogLevel.Debug)
                return;
            Console.ForegroundColor = ConsoleColor.Cyan;
            Conlog("(D) ", text, args);
        }

        /// <summary>
        /// Con Log
        /// </summary>
        /// <param name="prefix">Log Type prefix</param>
        /// <param name="text">text</param>
        /// <param name="args">args</param>
        private static void Conlog(string prefix, string text, params object[] args)
        {
            Console.Write(DateTime.Now.ToString("HH:mm:ss.ffff"));
            Console.Write(prefix);
            Console.WriteLine(text, args);
            Console.ResetColor();
        }
    }
}
