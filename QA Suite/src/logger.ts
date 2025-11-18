// src/logger.ts
import chalk from 'chalk';
import fs from 'node:fs';
import path from 'node:path';

// Default to showing info logs, but allow suppressing via env var
const SHOW_INFO_LOGS = process.env.QUIET_MODE !== 'true';
// Enable debug logs only when DEBUG=true
const SHOW_DEBUG_LOGS = process.env.DEBUG === 'true';

// Store logs in memory for later access
const logEntries: string[] = [];

// Create logs directory if it doesn't exist
const logsDir = path.join(process.cwd(), 'logs');
try {
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }
} catch (error) {
  console.error(`Failed to create logs directory: ${error}`);
}

// Generate a timestamped log filename
const now = new Date();
const datePart = now.toISOString().slice(0, 10); // YYYY-MM-DD
const timePart = now.toISOString().slice(11, 19).replace(/:/g, '-'); // HH-MM-SS
const logFilePath = path.join(logsDir, `qa-test-${datePart}-${timePart}.log`);

function stamp(level: string, colour: (s: string) => string, msg: string) {
  const ts = new Date().toISOString();
  const logMessage = `${level}:${ts} ${msg}`;
  
  // Store log entry in memory
  logEntries.push(logMessage);
  
  // Output colored log to console
  console.log(colour(`${level}:${ts}`), msg);
  
  // Append to log file
  try {
    fs.appendFileSync(logFilePath, logMessage + '\n');
  } catch (error) {
    console.error(`Failed to write to log file: ${error}`);
  }
}

export const log = {
  info: (msg: string) => {
    if (SHOW_INFO_LOGS) {
      stamp('INFO', chalk.cyan, msg);
    } else {
      // Still write to log file even if not showing in console
      const ts = new Date().toISOString();
      const logMessage = `INFO:${ts} ${msg}`;
      logEntries.push(logMessage);
      try {
        fs.appendFileSync(logFilePath, logMessage + '\n');
      } catch (error) {
        console.error(`Failed to write to log file: ${error}`);
      }
    }
  },
  debug: (msg: string) => {
    if (SHOW_DEBUG_LOGS) {
      stamp('DEBUG', chalk.blue, msg);
    } else {
      // Still write to log file even if not showing in console
      const ts = new Date().toISOString();
      const logMessage = `DEBUG:${ts} ${msg}`;
      logEntries.push(logMessage);
      try {
        fs.appendFileSync(logFilePath, logMessage + '\n');
      } catch (error) {
        console.error(`Failed to write to log file: ${error}`);
      }
    }
  },
  warn: (msg: string) => stamp('WARN', chalk.yellow, msg),
  error: (msg: string) => stamp('ERROR', chalk.red, msg),
  
  // Get full log contents as a string
  getLogContents: (): string => {
    return logEntries.join('\n');
  },
  
  // Get path to current log file
  getLogFilePath: (): string => {
    return logFilePath;
  }
};
