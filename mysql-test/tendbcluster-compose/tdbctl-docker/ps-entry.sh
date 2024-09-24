#!/bin/bash
#出错时候直接退出
set -eo pipefail
#设置nullglob, 开启后，如果正则未能匹配，则输出为空，而不是正则本身
#例如在空目录执行echo *，开启后输出为空，反之为输出*
shopt -s nullglob
shopt -s extglob

# if command starts with an option, prepend mysqld
#如果参数$1第一个字符是'-',则扩展为mysqld $@
#经过此逻辑处理后，docker run传进来的参数都变成mysqld --xxx
#${1:0:1} 表示从$1参数的第一个字符开始取，取第一个
#第一个1表示参数$1,第二0表示取成员的位置，第三个1表示成员个数,如果省略则全部取出
#例如${@:2} 表示取$@内容，位置从第3个成员开始(第一个是$0脚本名)
if [ "${1:0:1}" = '-' ]; then
	set -- mysqld "$@"
fi

# skip setup if they want an option that stops mysqld
#循环处理$@,是否打印help信息
wantHelp=
withCnf=
for arg; do
	case "$arg" in
		-'?'|--help|--print-defaults|-V|--version)
			wantHelp=1
			break
			;;
        --defaults-file*)
            withCnf=1
            ;;
	esac
done

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
#环境变量从文件或env中获取
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

# usage: process_init_file FILENAME MYSQLCOMMAND...
#    ie: process_init_file foo.sh mysql -uroot
# (process a single initializer file, based on its extension. we define this
# function here, so that initializer scripts (*.sh) can use the same logic,
# potentially recursively, or override the logic used in subsequent calls)
process_init_file() {
	local f="$1"; shift
	local mysql=( "$@" )

	case "$f" in
		*.sh)     echo "$0: running $f"; . "$f" ;;
		*.sql)    echo "$0: running $f"; "${mysql[@]}" < "$f"; echo ;;
		*.sql.gz) echo "$0: running $f"; gunzip -c "$f" | "${mysql[@]}"; echo ;;
		*)        echo "$0: ignoring $f" ;;
	esac
	echo
}

_check_config() {
	toRun=( "$@" --verbose --help --log-bin-index="$(mktemp -u)")
	if ! errors="$("${toRun[@]}" 2>&1 >/dev/null)"; then
		cat >&2 <<-EOM

			ERROR: mysqld failed while attempting to check config
			command was: "${toRun[*]}"

			$errors
		EOM
		exit 1
	fi
}

# Fetch value from server config
# We use mysqld --verbose --help instead of my_print_defaults because the
# latter only show values present in config files, and not server defaults
_get_config() {
	local conf="$1"; shift
	"$@" --verbose --help --log-bin-index="$(mktemp -u)" 2>/dev/null \
		| awk '$1 == "'"$conf"'" && /^[^ \t]/ { sub(/^[^ \t]+[ \t]+/, ""); print; exit }'
	# match "datadir      /some/path with/spaces in/it here" but not "--xyz=abc\n     datadir (xyz)"
}

_process_cnf_dir() {
	local conf="$1"
	local needDeal="$2"

  #Fetch value from defaults-file
  dir="$(_get_config "$conf" "${@:3}")"
  echo "process variable:$conf, get value: $dir"
  if [ ! -z "$dir" -a "$dir" != "(No default value)" ]; then
    if [ ! -z $needDeal ]; then
      #dir=$(dirname $dir|tr -d "\r")
      dir=$(dirname $dir)
    else
      dir=$(dirname $dir)/$(basename $dir)
    fi

    if [ "$dir" == "." ] || [ -d "$dir" ]; then
      return
    fi

    echo  "create directory $conf: $dir"
    mkdir -p "$dir"
    chown -R mysql $dir
  fi
}

echo "container run args:$@"
#-z条件 string长度为0则为真
if [ "$1" = 'mysqld' -a -z "$wantHelp" ]; then
	# still need to check config, container may have started with --user
  echo "do check configure"
	_check_config "$@"
  echo "check configure ok"

  DATADIR="$(_get_config 'datadir' "$@")"
  SOCKET="$(_get_config 'socket' "$@")"
  #if directory not empty and not include init-success flag file, mv all files to bak_$timestamp directory
  file_env 'MYSQL_INIT_COMPLETE'
  if [ ! -z "`ls -A $MYSQLDATADIR`" -a ! -f ${MYSQL_INIT_COMPLETE} ]; then
    cd $MYSQLDATADIR
    bakdir="bak_datadir_`date +%s`"
    mkdir -p ${bakdir}
    mv !(${bakdir}) $bakdir
  fi

  mkdir -p "$DATADIR"
  #create directory if not exists
  if [ ! -z "$withCnf" ]; then
    _process_cnf_dir 'log-bin' "want" "$@"
    _process_cnf_dir 'relay-log' "want" "$@"
    _process_cnf_dir 'socket' "want" "$@"
    _process_cnf_dir 'innodb-data-home-dir' "" "$@"
    _process_cnf_dir 'innodb-log-group-home-dir' "" "$@"
    _process_cnf_dir 'tmpdir' "" "$@"
    _process_cnf_dir 'slow-query-log-file' "want" "$@"
  fi

  if [ ! -d "$DATADIR/mysql" ]; then
		file_env 'MYSQL_ROOT_PASSWORD'
    #判断变量的值是否为空，为空返回0，为true
		if [ -z "$MYSQL_ROOT_PASSWORD" -a -z "$MYSQL_ALLOW_EMPTY_PASSWORD" -a -z "$MYSQL_RANDOM_ROOT_PASSWORD" ]; then
			echo >&2 'error: database is uninitialized and password option is not specified '
			echo >&2 '  You need to specify one of MYSQL_ROOT_PASSWORD, MYSQL_ALLOW_EMPTY_PASSWORD and MYSQL_RANDOM_ROOT_PASSWORD'
			exit 1
		fi

		echo "Initializing database, cmd: mysqld ${@:2}"
		# "Other options are passed to mysqld_safe." (so we pass all "mysqld" arguments directly here)
    cd /usr/local/mysql && ./bin/mysqld "${@:2}" --initialize-insecure
		echo 'Database initialized'

    chown -R mysql ${MYSQLDATADIR}
		"$@" --skip-networking --socket="${SOCKET}" --tc_admin=0 &
    pid="$!"

    mysql=( mysql --protocol=socket -uroot -hlocalhost --socket="${SOCKET}" )

		for i in {120..0}; do
			if echo 'SELECT 1' | "${mysql[@]}" &> /dev/null; then
				break
			fi
			echo 'MySQL init process in progress...'
			sleep 1
		done
		if [ "$i" = 0 ]; then
			echo >&2 'MySQL init process failed.'
			exit 1
		fi

		if [ -z "$MYSQL_INITDB_SKIP_TZINFO" ]; then
			# sed is for https://bugs.mysql.com/bug.php?id=20545
			mysql_tzinfo_to_sql /usr/share/zoneinfo | sed 's/Local time zone must be set--see zic manual page/FCTY/' | "${mysql[@]}" mysql
		fi

		if [ ! -z "$MYSQL_RANDOM_ROOT_PASSWORD" ]; then
			MYSQL_ROOT_PASSWORD="$(pwmake 128)"
			echo "GENERATED ROOT PASSWORD: $MYSQL_ROOT_PASSWORD"
		fi

		rootCreate=
		# default root to listen for connections from anywhere
		file_env 'MYSQL_ROOT_HOST' '%'
		if [ ! -z "$MYSQL_ROOT_HOST" -a "$MYSQL_ROOT_HOST" != 'localhost' ]; then
			# no, we don't care if read finds a terminating character in this heredoc
			# https://unix.stackexchange.com/questions/265149/why-is-set-o-errexit-breaking-this-read-heredoc-expression/265151#265151
			read -r -d '' rootCreate <<-EOSQL || true
				CREATE USER 'root'@'${MYSQL_ROOT_HOST}' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}' ;
				GRANT ALL ON *.* TO 'root'@'${MYSQL_ROOT_HOST}' WITH GRANT OPTION ;
			EOSQL
		fi

		"${mysql[@]}" <<-EOSQL
			-- What's done in this file shouldn't be replicated
			--  or products like mysql-fabric won't work
			SET @@SESSION.SQL_LOG_BIN=0;
			DELETE FROM mysql.user WHERE user NOT IN ('mysql.sys', 'root','mysql.session') OR host NOT IN ('localhost') ;
			SET PASSWORD FOR 'root'@'localhost'=PASSWORD('${MYSQL_ROOT_PASSWORD}') ;
			GRANT ALL ON *.* TO 'root'@'localhost' WITH GRANT OPTION ;
			${rootCreate}
			FLUSH PRIVILEGES ;
		EOSQL

		if [ ! -z "$MYSQL_ROOT_PASSWORD" ]; then
			mysql+=( -p"${MYSQL_ROOT_PASSWORD}" )
		fi

		file_env 'MYSQL_ADMIN_USER'
		if [ ! -z "$MYSQL_ADMIN_USER" ]; then
		    file_env 'MYSQL_ADMIN_PASS'
		    if [ -z "$MYSQL_ADMIN_PASS" ]; then
			    echo >&2 'error : you need to specify MYSQL_ADMIN_PASS if MYSQL_ADMIN_USER exits'
				exit 1
		    fi
			echo "CREATE USER '$MYSQL_ADMIN_USER'@'localhost' IDENTIFIED BY '$MYSQL_ADMIN_PASS' ;" | "${mysql[@]}"
			echo "GRANT ALL ON *.* TO '${MYSQL_ADMIN_USER}'@'localhost' WITH GRANT OPTION ;" | "${mysql[@]}"
		fi

    file_env 'MYSQL_OPERATOR_USER'
    if [ ! -z "$MYSQL_OPERATOR_USER" ]; then
      file_env 'MYSQL_OPERATOR_PASS'
      if [ -z "$MYSQL_OPERATOR_PASS" ]; then
        echo >&2 'error : you need to specify MYSQL_OPERATOR_PASS or MYSQL_OPERATOR_HOST if MYSQL_OPERATOR_USER exits'
        exit 1
      fi
      file_env 'MYSQL_OPERATOR_HOST'
      if [ ! -z "$MYSQL_OPERATOR_HOST" -a "$MYSQL_OPERATOR_HOST" != 'localhost' ]; then
        echo "CREATE USER '$MYSQL_OPERATOR_USER'@'$MYSQL_OPERATOR_HOST' IDENTIFIED BY '$MYSQL_OPERATOR_PASS' ;" | "${mysql[@]}"
        echo "GRANT ALL ON *.* TO '${MYSQL_OPERATOR_USER}'@'$MYSQL_OPERATOR_HOST' WITH GRANT OPTION ;" | "${mysql[@]}"
      fi
      echo "CREATE USER '$MYSQL_OPERATOR_USER'@'localhost' IDENTIFIED BY '$MYSQL_OPERATOR_PASS' ;" | "${mysql[@]}"
      echo "GRANT ALL ON *.* TO '${MYSQL_OPERATOR_USER}'@'localhost' WITH GRANT OPTION ;" | "${mysql[@]}"
    fi

		echo
		ls /docker-entrypoint-initdb.d/ > /dev/null
		for f in /docker-entrypoint-initdb.d/*; do
			process_init_file "$f" "${mysql[@]}"
		done

		"${mysql[@]}" <<-EOSQL
			SET @@SESSION.SQL_LOG_BIN=0;
			CREATE USER 'healthchecker'@'localhost' IDENTIFIED BY 'healthcheckpass';
			GRANT super ON *.* TO 'healthchecker'@'localhost';
			grant select ,insert ,update,delete on db_infobase.check_heartbeat to 'healthchecker'@'localhost';
			FLUSH PRIVILEGES ;
		EOSQL

		if [ ! -z "$MYSQL_ONETIME_PASSWORD" ]; then
			"${mysql[@]}" <<-EOSQL
				DROP USER 'root'@'%';
			EOSQL
		fi
		if ! kill -s TERM "$pid" || ! wait "$pid"; then
			echo >&2 'MySQL init process failed.'
			exit 1
		fi

		echo
		echo 'MySQL init process done. Ready for start up.'
		echo
    else
        echo "directory $DATADIR/mysql exist, skip init"
    fi

    chown -R mysql $MYSQLDATADIR
    chown -R mysql $DATADIR

    #修改启动命令
    file_env "MYSQLDIR"
    cd ${MYSQLDIR}
    startCmd="./bin/mysqld_safe"
    set -- $startCmd "${@:2} --tc_admin=1"

    file_env "MYSQLDATADIR"
    gosu mysql touch ${MYSQLDATADIR}/healthcheck.cnf
    cat >"${MYSQLDATADIR}/healthcheck.cnf" <<EOF
[client]
user=healthchecker
socket=${SOCKET}
password=healthcheckpass
EOF
    gosu mysql touch ${MYSQL_INIT_COMPLETE}
	  echo "[Entrypoint] Starting ${CONTROL_VERSION}:${@}"
fi

exec $@
