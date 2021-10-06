# git clone 
FROM centos:centos8.3.2011
# 设置匿名卷， 应该被覆盖
#set up basic build environment
RUN yum install -y automake autoconf libtool make gcc gcc-c++ gdb libarchive cmake gdb-gdbserver && \
    yum install -y libpmem libpmem-devel libpmemobj libpmemobj-devel libvmem libpmemobj++-devel &&\
    yum install -y numactl &&\
    yum install -y git vim &&\
    yum install -y fuse fuse-common fuse-devel fuse-libs fuse-overlayfs && \
	yum install -y numactl-devel &&\
	yum install -y boost boost-thread boost-devel libatomic &&\
    mkdir /home/src &&\
    mkdir /mnt/pmem 

RUN sed -ri 's/enabled=0/enabled=1/g'  /etc/yum.repos.d/CentOS-Linux-PowerTools.repo && \
    yum install -y gflags gflags-devel zlib zlib-devel snappy snappy-devel tbb tbb-devel

# gdb
RUN sed -ri 's/enabled=0/enabled=1/g' /etc/yum.repos.d/CentOS-Linux-Debuginfo.repo && \ 
    yum install -y glibc && \
    yum debuginfo-install -y glibc-2.28-151.el8.x86_64 && \
    yum debuginfo-install -y boost-system-1.66.0-10.el8.x86_64 boost-thread-1.66.0-10.el8.x86_64 daxctl-libs-71.1-2.el8.x86_64 kmod-libs-25-16.el8.x86_64 libatomic-8.4.1-1.el8.x86_64 libblkid-2.32.1-24.el8.x86_64 libgcc-8.4.1-1.el8.x86_64 libmount-2.32.1-24.el8.x86_64 libpmem-1.6.1-1.el8.x86_64 libpmemobj-1.6.1-1.el8.x86_64 libselinux-2.9-4.el8_3.x86_64 libstdc++-8.4.1-1.el8.x86_64 libuuid-2.32.1-24.el8.x86_64 ndctl-libs-71.1-2.el8.x86_64 openssl-libs-1.1.1g-15.el8_3.x86_64 pcre2-10.32-2.el8.x86_64 systemd-libs-239-41.el8_3.x86_64 tbb-2018.2-9.el8.x86_64 xz-libs-5.2.4-3.el8.x86_64 zlib-1.2.11-17.el8.x86_64

# set up remote access
RUN yum install -y openssl openssh-server rsync && \
    sed -ri 's/^PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config &&\
    cmake --version && \
    g++ -v && \
    echo 'root:ganquan123' | chpasswd && \
    mkdir /var/run/sshd

RUN ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N "" && \
    ssh-keygen -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key -N "" && \
    ssh-keygen -t ed25519 -f /etc/ssh/ssh_host_ed25519_key -N "" && \
    sh -c '/bin/echo -e "\nyes\nyes" | ssh-keygen -t rsa'
# 声明容器运行时提供服务的端口，这只是一个声明，在容器运行时并不直接开启端口服务
EXPOSE 22
#CMD ["/usr/bin/rsync --daemon --config=/etc/rsync.conf"]
CMD ["/usr/sbin/sshd", "-D"]
#CMD ["/bin/bash", "-D"]