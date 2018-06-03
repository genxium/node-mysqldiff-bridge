#!/usr/bin/env node

const baseAbsPath = __dirname + '/';
const Promise = require('bluebird');
const util = require('util');
const mysql = require('mysql2');
const Logger = require('./utils/Logger');
const logger = Logger.instance.getLogger(__filename);
const fs = require('fs');
const child_process = require('child_process');
const path = require('path');

const ArgumentParser = require('argparse').ArgumentParser;
const parser = new ArgumentParser({
  version: '0.0.1',
  addHelp: true,
  description: 'mysqldiff-bridge'
});
parser.addArgument(
  ['--tab'],
  {
    defaultValue: baseAbsPath + './skeema-repo-root',
    required: false,
    help: 'Path to the directiory containing <tablename>.sql files to be loaded into the tmp database, usually created by `mysqldump --no-data --tab=<...>` or `skeema pull`.'
  }
);
parser.addArgument(
  ['--host'],
  {
    defaultValue: 'localhost',
    required: false,
    help: 'Mysql server host.'
  }
);
parser.addArgument(
  ['--port'],
  {
    defaultValue: 3306,
    required: false,
    help: 'Mysql server port.'
  }
);
parser.addArgument(
  ['--user'],
  {
    defaultValue: 'root',
    required: false,
    help: 'Mysql server username.'
  }
);
parser.addArgument(
  ['--password'],
  {
    defaultValue: null,
    required: false,
    help: 'Mysql server raw password.'
  }
);
parser.addArgument(
  'livedbname',
  {
    help: 'Mysql server live dbname to be compared with the tmp database.'
  }
);
parser.addArgument(
  'direction',
  {
    help: 'Takes `push` or `pull`, indicating whether you\'re about to push the `tabbed schema files` into livedb or pull your livedb into `tabbed schema files`.'
  }
);
parser.addArgument(
  ['--retainsTmp'],
  {
    defaultValue: true,
    required: false,
    help: 'Whether the database named `tmp` should be retained after each round of `push` operation.'
  }
);
parser.addArgument(
  ['--pushScriptExportPath'],
  {
    defaultValue: null,
    required: false,
    help: 'The absolute path of file into which you want to export the push script.'
  }
);
const args = parser.parseArgs();

const sqlFileDirFullPath = (/\/$/.test(args.tab) ? args.tab : util.format("%s/", args.tab));
const mysqlServerHost = args.host;
const mysqlServerPort = args.port;
const mysqlServerUser = args.user;
const mysqlServerRawPassword = args.password;
const mysqlServerLiveDbname = args.livedbname;

const mysqlServerTmpDbname = 'tmp';

logger.info(util.format("Using {\n\tsqlFileDirFullPath: %s\n\tmysqlServerHost: %s\n\tmysqlServerPort: %s\n\tmysqlServerUser: %s\n\tmysqlServerRawPassword: %s\n\tmysqlServerLiveDbname: %s\n}", sqlFileDirFullPath, mysqlServerHost, mysqlServerPort, mysqlServerUser, mysqlServerRawPassword, mysqlServerLiveDbname));

// create the connection to database
const connectionTmpDb = (
null == mysqlServerRawPassword
  ?
  mysql.createConnection({
    host: mysqlServerHost,
    user: mysqlServerUser,
  }) : mysql.createConnection({
    host: mysqlServerHost,
    user: mysqlServerUser,
    passwordSha1: Buffer.from(mysqlServerRawPassword, 'hex')
  })
);
const connectionLiveDb = (
null == mysqlServerRawPassword
  ?
  mysql.createConnection({
    host: mysqlServerHost,
    user: mysqlServerUser,
    database: mysqlServerLiveDbname,
  }) : mysql.createConnection({
    host: mysqlServerHost,
    user: mysqlServerUser,
    database: mysqlServerLiveDbname,
    passwordSha1: Buffer.from(mysqlServerRawPassword, 'hex')
  })
);

const dropDbIfExistsStmt = util.format("DROP DATABASE IF EXISTS %s", mysqlServerTmpDbname);
const createDbStmt = util.format("CREATE DATABASE %s", mysqlServerTmpDbname);
logger.info(util.format("About to execute\n\t%s\n\t%s", dropDbIfExistsStmt, createDbStmt));

const useTmpDbStmt = util.format("USE %s", mysqlServerTmpDbname);
const useLiveDbStmt = util.format("USE %s", mysqlServerLiveDbname);

const sqlFilenameRegexp = /\.sql$/;
function mergeTwoListsSync(firstOperandList, secondOperandList) {
  let resultSet = new Set();
  firstOperandList.map(function(eleVal) {
    resultSet.add(eleVal);
  });
  secondOperandList.map(function(eleVal) {
    if (resultSet.has(eleVal)) return;
    resultSet.add(eleVal);
  });
  return [...resultSet];
}

function getExclusiveImageRemoteNameListInFirstOperandSync(firstOperandList, secondOperandList) {
  let secondSet = new Set();
  secondOperandList.map(function(eleVal) {
    secondSet.add(eleVal);
  });
  let resultList = [];
  firstOperandList.map(function(eleVal) {
    if (secondSet.has(eleVal)) return;
    resultList.push(eleVal);
  });
  return resultList;
}

function isSqlFileSync(fileFullPath) {
  const isFile = fs.lstatSync(fileFullPath).isFile();
  if (!isFile) {
    logger.info(util.format("%s is not a file.", fileFullPath));
    return false;
  }
  const hasSqlExtension = sqlFilenameRegexp.test(fileFullPath);
  if (!hasSqlExtension) {
    logger.info(util.format("%s doesn't have `.sql` extension.", fileFullPath));
    return false;
  }
  return true;
}

function dryRunPush() {
  let pushScript = "";

  let versionControlledTableNameList = [];
  let livedbTableNameList = [];

  let mergedTableNameList = [];
  let exclusiveVersionControlledTableNameList = [];
  let exclusiveLivedbTableNameList = [];

  new Promise(function(resolve, reject) {
    connectionLiveDb.query(
      "SHOW TABLES",
      function(err, results, fields) {
        if (undefined !== err && null !== err) {
          logger.error(err);
          resolve(false);
        } else {
          results.map(function(result) {
            const trickyKey = Object.keys(result)[0]; // A little hack.
            const tableName = result[trickyKey];
            livedbTableNameList.push(tableName);
          });
          resolve(true);
        }
      }
    );
  })
    .then(function(trueOrFalse) {
      return new Promise(function(resolve, reject) {
        connectionTmpDb.query(
          dropDbIfExistsStmt,
          function(err, results, fields) {
            if (undefined !== err && null !== err) {
              logger.error(err);
              resolve(false);
            } else {
              logger.debug(util.format("result: %o, fields: %o", results, fields));
              resolve(true);
            }
          }
        );
      });
    })
    .then(function(trueOrFalse) {
      if (false == trueOrFalse)
        throw new Error();
      logger.info(util.format("About to create database %s.", mysqlServerTmpDbname));
      return new Promise(function(resolve, reject) {
        connectionTmpDb.query(
          createDbStmt,
          function(err, results, fields) {
            if (undefined !== err && null !== err) {
              logger.error(err);
              resolve(false);
            } else {
              logger.debug(util.format("result: %o, fields: %o", results, fields));
              resolve(true);
            }
          }
        );
      });
    })
    .then(function(trueOrFalse) {
      if (false == trueOrFalse)
        throw new Error();
      logger.info(util.format("About to use database %s.", mysqlServerTmpDbname));
      return new Promise(function(resolve, reject) {
        connectionTmpDb.query(
          useTmpDbStmt,
          function(err, results, fields) {
            if (undefined !== err && null !== err) {
              logger.error(err);
              resolve(false);
            } else {
              logger.debug(util.format("result: %o, fields: %o", results, fields));
              resolve(true);
            }
          }
        );
      });
    })
    .then(function(trueOrFalse) {
      if (false == trueOrFalse)
        throw new Error();
      return new Promise(function(resolve, reject) {
        fs.readdir(sqlFileDirFullPath, (err, files) => {
          if (err) {
            resolve(null);
          } else {
            resolve(files);
          }
        });
      });
    })
    .then(function(files) {
      if (undefined === files || null === files)
        throw new Error();
      if (0 == files.length)
        throw new Error();

      files.map(function(file) {
        const tableName = path.basename(file, '.sql');
        versionControlledTableNameList.push(tableName);
      });

      mergedTableNameList = mergeTwoListsSync(versionControlledTableNameList, livedbTableNameList);
      exclusiveVersionControlledTableNameList = getExclusiveImageRemoteNameListInFirstOperandSync(versionControlledTableNameList, livedbTableNameList);
      exclusiveLivedbTableNameList = getExclusiveImageRemoteNameListInFirstOperandSync(livedbTableNameList, versionControlledTableNameList);

      logger.info(util.format("versionControlledTableNameList: %o\nlivedbTableNameList: %o\nmergedTableNameList: %o\nexclusiveVersionControlledTableNameList: %o\nexclusiveLivedbTableNameList: %o", versionControlledTableNameList, livedbTableNameList, mergedTableNameList, exclusiveVersionControlledTableNameList, exclusiveLivedbTableNameList));

      return Promise.reduce(files, function(total, file) {
        const fileFullPath = (sqlFileDirFullPath + file);
        if (!isSqlFileSync(fileFullPath)) return ++total;
        logger.info(util.format("About to source %s with database %s.", file, mysqlServerTmpDbname));
        const sqlStr = fs.readFileSync(fileFullPath, "utf8");
        return new Promise(function(resolve, reject) {
          connectionTmpDb.query(
            sqlStr,
            function(err, results, fields) {
              if (undefined !== err && null !== err) {
                logger.error(err);
                logger.warn(util.format("Not sourced."));
                resolve(false);
              } else {
                logger.info(util.format("Sourced."));
                resolve(true);
              }
            }
          );
        })
          .then(function(trueOrFalse) {
            return ++total;
          })
      }, 0);
    })
    .then(function(total) {
      return Promise.reduce(mergedTableNameList, function(total, tableName) {
        if (-1 != exclusiveVersionControlledTableNameList.indexOf(tableName)) {
          logger.info(util.format("This is a new table to be created in the livedb."));
          const fileFullPath = (sqlFileDirFullPath + tableName + ".sql");
          const sqlStr = fs.readFileSync(fileFullPath, "utf8");
          pushScript += util.format("%s\n", sqlStr);
          return ++total;
        } else if (-1 != exclusiveLivedbTableNameList.indexOf(tableName)) {
          logger.info(util.format("This is a table to be dropped from the livedb."));
          const dropTableIfExistsStmt = util.format("DROP TABLE IF EXISTS %s", tableName);
          pushScript += util.format("%s\n", dropTableIfExistsStmt);
          return ++total;
        } else {
          logger.info(util.format("About to find the alter script for `%s.%s - %s.%s`", mysqlServerTmpDbname, tableName, mysqlServerLiveDbname, tableName));
          let cmdToRun = null;
          if (null == mysqlServerRawPassword) {
            cmdToRun = util.format("mysqldiff --server1=%s@%s:%s --compact --difftype=sql %s.%s:%s.%s", mysqlServerUser, mysqlServerHost, mysqlServerPort, mysqlServerLiveDbname, tableName, mysqlServerTmpDbname, tableName);
          } else {
            cmdToRun = util.format("mysqldiff --server1=%s:'%s'@%s:%s --compact --difftype=sql %s.%s:%s.%s", mysqlServerUser, mysqlServerRawPassword, mysqlServerHost, mysqlServerPort, mysqlServerLiveDbname, tableName, mysqlServerTmpDbname, tableName);
          }
          return new Promise(function(resolve, reject) {
            const chdproc = child_process.exec(cmdToRun, function(err, stdoutResult, stderrResult) {
              pushScript += util.format("%s\n", stdoutResult);
              resolve(true);
            });
          })
          .then(function(trueOrFalse) {
            return ++total;
          });
        }
      }, 0);
    })
    .then(function(total) {
      logger.info(util.format("The final pushScript is\n%s", pushScript));
    })
    .catch(function(ex) {
      logger.error(ex);
    })
    .finally(function() {
      if (args.retainsTmp) {
        process.exit();
      } else {
        logger.info(util.format("About to execute\n\t%s", dropDbIfExistsStmt));
        connectionTmpDb.query(
          dropDbIfExistsStmt,
          function(err, results, fields) {
            if (undefined !== err && null !== err) {
              logger.error(err);
            } else {
              logger.debug(util.format("result: %o, fields: %o", results, fields));
            }
            process.exit();
          }
        );
      }
    });
}

function dryRunPull() {
  new Promise(function(resolve, reject) {
    fs.readdir(sqlFileDirFullPath, (err, files) => {
      if (err) {
        resolve(null);
      } else {
        resolve(files);
      }
    });
  })
    .then(function(files) {
      if (undefined === files || null === files)
        throw new Error();
      return Promise.reduce(files, function(total, file) {
        const fileFullPath = (sqlFileDirFullPath + file);
        if (!isSqlFileSync(fileFullPath)) return ++total;
        fs.unlinkSync(fileFullPath);
        return ++total;
      }, 0);
    })
    .then(function(total) {
      logger.info(util.format("Just delete %d `*.sql` files.", total));
      let cmdToRun = null;
      if (null == mysqlServerRawPassword) {
        cmdToRun = util.format("mysqldump --host=%s --port=%s --user=%s --no-data --tab=%s --skip-add-drop-table --skip-add-locks %s", mysqlServerHost, mysqlServerPort, mysqlServerUser, sqlFileDirFullPath, mysqlServerLiveDbname);
      } else {
        cmdToRun = util.format("mysqldump --host=%s --port=%s --user=%s --password=%s --no-data --tab=%s --skip-add-drop-table --skip-add-locks %s", mysqlServerHost, mysqlServerPort, mysqlServerUser, mysqlServerRawPassword, sqlFileDirFullPath, mysqlServerLiveDbname);
      }
      const stdoutRet = child_process.execSync(cmdToRun);
      logger.info(util.format("Result of executing %s\n%s", cmdToRun, stdoutRet));
    })
    .catch(function(ex) {
      logger.error(ex);
    })
    .finally(function() {
      process.exit();
    });
}

if ('pull' == args.direction) {
  dryRunPull();
}

if ('push' == args.direction) {
  dryRunPush();
}