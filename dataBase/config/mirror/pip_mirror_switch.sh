#!/bin/bash

PIP_MIRRORS=(
    "https://pypi.tuna.tsinghua.edu.cn/simple"
    "https://mirrors.aliyun.com/pypi/simple"
    "https://pypi.douban.com/simple"
    "https://pypi.mirrors.ustc.edu.cn/simple"
    "https://repo.huaweicloud.com/repository/pypi/simple"
    "https://mirrors.cloud.tencent.com/pypi/simple"
    "https://mirrors.bfsu.edu.cn/pypi/web/simple"
)

test_mirror() {
    local mirror=$1
    echo "Testing mirror: $mirror" >&2
    timeout 10 curl -s --connect-timeout 5 --head "$mirror" | head -1 | grep -E "200|301|302" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        return 0
    fi
    timeout 10 wget -q --spider --timeout=5 "$mirror" > /dev/null 2>&1
    return $?
}

select_best_mirror() {
    echo "Testing available pip mirrors..." >&2
    for mirror in "${PIP_MIRRORS[@]}"; do
        if test_mirror "$mirror"; then
            echo "✓ Mirror $mirror is available" >&2
            echo "$mirror"
            return 0
        else
            echo "✗ Mirror $mirror is not available" >&2
        fi
    done
    echo "All mirrors failed, using default" >&2
    echo "https://pypi.org/simple"
    return 1
}

configure_pip() {
    local best_mirror=$(select_best_mirror)
    echo "Configuring pip to use: $best_mirror"
    
    mkdir -p "$HOME/.config/pip"
    
    if [ -f "$HOME/.config/pip/pip.conf" ]; then
        mv "$HOME/.config/pip/pip.conf" "$HOME/.config/pip/pip.conf.backup"
        echo "Backed up existing pip.conf to pip.conf.backup"
    fi
    
    cat > "$HOME/.config/pip/pip.conf" << EOF
[global]
index-url = $best_mirror
extra-index-url =
    https://mirrors.aliyun.com/pypi/simple/
    https://pypi.douban.com/simple/
    https://pypi.mirrors.ustc.edu.cn/simple/
    https://repo.huaweicloud.com/repository/pypi/simple/
    https://mirrors.cloud.tencent.com/pypi/simple/
    https://mirrors.bfsu.edu.cn/pypi/web/simple/
trusted-host =
    pypi.tuna.tsinghua.edu.cn
    mirrors.aliyun.com
    pypi.douban.com
    pypi.mirrors.ustc.edu.cn
    repo.huaweicloud.com
    mirrors.cloud.tencent.com
    mirrors.bfsu.edu.cn
timeout = 30

[install]
trusted-host =
    pypi.tuna.tsinghua.edu.cn
    mirrors.aliyun.com
    pypi.douban.com
    pypi.mirrors.ustc.edu.cn
    repo.huaweicloud.com
    mirrors.cloud.tencent.com
    mirrors.bfsu.edu.cn
EOF
    
    echo "Pip mirror configured successfully!"
    python3 -m pip config list
}

if [ "$1" = "test" ]; then
    select_best_mirror
elif [ "$1" = "configure" ]; then
    configure_pip
else
    echo "Usage: $0 {test|configure}"
    echo "  test        - Test all mirrors and find the best one"
    echo "  configure   - Auto-detect and configure the best pip mirror"
fi