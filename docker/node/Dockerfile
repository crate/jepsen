# FIXME: tutum/debian will be deprecated soon: https://github.com/tutumcloud/tutum-debian/blob/master/README.md
FROM tutum/debian:jessie

RUN rm /etc/apt/apt.conf.d/docker-clean && apt-get update && apt-get install -y bzip2 curl faketime iproute iptables iputils-ping libzip2 logrotate man man-db net-tools ntpdate psmisc python rsyslog sudo sysvinit sysvinit-core sysvinit-utils tar unzip vim wget && apt-get remove -y --purge --auto-remove systemd
RUN sed -i.bak s/'LogLevel INFO'/'LogLevel Verbose'/g /etc/ssh/sshd_config
RUN service ssh restart
