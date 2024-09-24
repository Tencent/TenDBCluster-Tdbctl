FROM centos:7
#MAINTAINER TenDB Development <jamxia@tencent.com>

RUN groupadd -r mysql -g 202 && useradd -m -r -g 202 -d /home/mysql -G users -u 30019 mysql


ENV CONTROL_VERSION tdbctl-test
ENV PATH=$PATH:/usr/local/mysql/bin
ENV MYSQLDIR=/usr/local/mysql MYSQLDATADIR=/data1/
ENV MYSQL_INIT_COMPLETE=${MYSQLDATADIR}/mysql_init_complete
ENV TERM=xterm
ENV TZ=Asia/Shanghai

COPY bin/gosu /usr/bin/gosu
COPY ${CONTROL_VERSION}-linux-x86_64.tar.gz ${CONTROL_VERSION}.tar.gz
COPY my.cnf /etc/my.cnf
COPY init-sql/* /docker-entrypoint-initdb.d/
COPY healthcheck.sh /usr/bin/healthcheck.sh
COPY ps-entry.sh /docker-entrypoint.sh

RUN rpm -ivh http://mirrors.tencent.com/centos/7/os/x86_64/Packages/wget-1.14-18.el7_6.1.x86_64.rpm  \
    && mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup \
    && wget -O /etc/yum.repos.d/CentOS-Base.repo https://mirrors.tencent.com/repo/centos7_base.repo \
    && yum clean all \
    && yum makecache \
    && yum install -y \
    numactl.x86_64 \
    libaio.x86_64 \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && chmod +x /usr/bin/gosu \
    && gosu nobody true \
    && /usr/bin/install -m 0775 -o mysql -g root -d /usr/local/mysql ${MYSQLDATADIR} /docker-entrypoint-initdb.d \
    && tar xzf ${CONTROL_VERSION}.tar.gz -C ${MYSQLDIR} --strip-components=1 \
    && rm ${CONTROL_VERSION}.tar.gz \
    && chown -R mysql ${MYSQLDIR} \
    && chown -R mysql /docker-entrypoint.sh \
    && chown -R mysql /etc/my.cnf

VOLUME ${MYSQLDATADIR}

ENTRYPOINT ["/docker-entrypoint.sh"]
HEALTHCHECK --interval=15s --timeout=5s CMD healthcheck.sh

EXPOSE 26000
CMD ["mysqld"]
