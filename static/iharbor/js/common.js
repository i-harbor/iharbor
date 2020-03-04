
//
// 显示一个自动关闭提示对话框
//type: 提示类型; can be success, warning, info, error, question
//title: 提示文本
//position: 对话框弹出位置; can be 'top', 'top-start', 'top-end', 'center', 'center-start', 'center-end',
//          'bottom', 'bottom-start', or 'bottom-end'
//timer: 自动关闭定时，单位毫秒
function show_auto_close_warning_dialog(title, type='warning', position='center', timer=1500) {
    let showBtn = false;
    if(timer > 0){}
    else{
        timer = 0;
        showBtn = true;
    }
    return swal({
        position: position,
        type: type,
        text: title,
        showConfirmButton: showBtn,
        timer: timer
    });
}

//
// 显示一个提示对话框
//type: 提示类型; can be success, warning, info, error, question
//title: 提示文本
//position: 对话框弹出位置; can be 'top', 'top-start', 'top-end', 'center', 'center-start', 'center-end',
//          'bottom', 'bottom-start', or 'bottom-end'
function show_warning_dialog(title, type='warning', position='center') {
    return show_auto_close_warning_dialog(title, type, position, 0);
}


function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = jQuery.trim(cookies[i]);
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

function csrfSafeMethod(method) {
    // these HTTP methods do not require CSRF protection
    return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
}

//
// 动态修改浏览器url,而不刷新网页
//
function change_url_no_refresh(new_url, new_title) {
    var state = {
        title: document.title,
        url: document.location.href,
        otherkey: null
    };
    history.replaceState(state, new_title, new_url);
}

/**
 * 去除字符串前后给定字符，不改变原字符串
 * @param char
 * @returns { String }
 */
String.prototype.strip = function (char) {
  if (char){
    return this.replace(new RegExp('^\\'+char+'+|\\'+char+'+$', 'g'), '');
  }
  return this.replace(/^\s+|\s+$/g, '');
};

//返回一个去除右边的给定字符的字符串，不改变原字符串
String.prototype.rightStrip = function(searchValue){
    if(this.endsWith(searchValue)){
        return this.substring(0, this.lastIndexOf(searchValue));
    }
    return this;
};

//返回一个去除左边的给定字符的字符串，不改变原字符串
String.prototype.leftStrip = function(searchValue){
    if(this.startsWith(searchValue)){
        return this.replace(searchValue);
    }
    return this;
};

//
// 弹出一个确认对话框
// 输入参数：一个对象
// @title: 标题
// @text：显示文本
// @ok_todo：确定 回调函数
// @cancel_todo：取消 回调函数
function show_confirm_dialog(obj={title:"", text:"", ok_todo:null, cancel_todo:null}) {
    Swal({
        title: obj.title || "你确定要这样做吗？",
        text: obj.text || "此操作是不可逆的！",
        type: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#3085d6',
        cancelButtonColor: '#d33',
        confirmButtonText: '确定',
        cancelButtonText: '取消'
    }).then((result) => {
        if (result.value) {
            if(typeof obj.ok_todo === "function")//是函数
                obj.ok_todo();
        }
        else{
            if(typeof obj.cancel_todo === "function")//是函数
                obj.cancel_todo();
        }
    })
}

//
//form 表单获取所有数据 封装方法
//
function getFormJson(form_node) {
    let o = {};
    let a = $(form_node).serializeArray();
    $.each(a, function () {
        if (o[this.name] !== undefined) {
            if (!o[this.name].push) {
                o[this.name] = [o[this.name]];
            }
            o[this.name].push(this.value || '');
        } else {
            o[this.name] = this.value || '';
        }
    });

    return o;
}

//
// 从当前url中获取域名
// 如http://abc.com/
function get_domain_url() {
    let origin = window.location.origin;
    origin = origin.rightStrip('/');
    return origin + '/';
}

/**
 * 拼接params对象为url参数字符串
 * @param {Object} obj - 待拼接的对象
 * @returns {string} - 拼接成的请求字符串
 */
function encode_params(obj) {
    const params = [];

    Object.keys(obj).forEach((key) => {
        let value = obj[key];
        // 如果值为undefined我们将其置空
        if (typeof value === "undefined") {
            value = ''
        }
        // 对于需要编码的文本我们要进行编码
        params.push([key, encodeURIComponent(value)].join('='))
    });

    return params.join('&');
}

function sizeFormat(val, unit) {
    if (isNaN(val))
        return val;
    let value = val;
    let mb = 1 << 20;
    if (unit === "B") {
        if (val > mb) {
            value = val / mb;
            return sizeFormat(value, 'MB');
        }
    }
    switch (unit) {
        case "B":
            if (val > 1024) {
                val = val / 1024;
                value = sizeFormat(val, 'KB');
            } else {
                value = val.toFixed(2) + 'B';
            }
            break;
        case "KB":
            if (val > 1024) {
                val = val / 1024;
                value = sizeFormat(val, 'MB');
            } else {
                value = val.toFixed(2) + 'KB';
            }
            break;
        case "MB":
            if (val > 1024) {
                val = val / 1024;
                value = sizeFormat(val, 'GB');
            } else {
                value = val.toFixed(2) + 'MB';
            }
            break;
        case "GB":
            if (val > 1024) {
                val = val / 1024;
                value = sizeFormat(val, 'TB');
            } else {
                value = val.toFixed(2) + 'GB';
            }
            break;
        case "TB":
            if (val > 1024) {
                val = val / 1024;
                value = sizeFormat(val, 'PB');
            } else {
                value = val.toFixed(2) + 'TB';
            }
            break;
        case "PB":
            value = val.toFixed(2) + 'PB';
            break;
        default:
            value = val.toFixed(2) + 'B';
    }

    return value
}

// 将 Date 转化为指定格式的String
// 月(M)、日(d)、小时(H)、分(m)、秒(s)、季度(q) 可以用 1-2 个占位符，
// 年(y)可以用 1-4 个占位符，毫秒(S)只能用 1 个占位符(是 1-3 位的数字)
// 例子：
// dateFormat("yyyy-MM-dd HH:mm:ss.S", date) ==> 2006-07-02 08:09:04.423
// dateFormat("yyyy-M-d H:m:s.S", date)      ==> 2006-7-2 8:9:4.18
function dateFormat(fmt, date) {
    let ret;
    let opt = {
        "y+": date.getFullYear().toString(),
        "M+": (date.getMonth() + 1).toString(),
        "d+": date.getDate().toString(),
        "H+": date.getHours().toString(),
        "m+": date.getMinutes().toString(),
        "s+": date.getSeconds().toString(),
        "S+": date.getMilliseconds().toString()
    };
    for (let k in opt) {
        ret = new RegExp("(" + k + ")").exec(fmt);
        if (ret) {
            fmt = fmt.replace(ret[1], (ret[1].length === 1) ? (opt[k]) : (opt[k].padStart(ret[1].length, "0")))
        }
    }
    return fmt;
}

// iso格式时间转本地时间
// "'2020-03-04T09:09:49.032064+07:00'" ==> ""2020-03-04 10:09:49""
function isoTimeToLocal(isoTime) {
    let lTime;
    if (!isoTime){
        return isoTime;
    }
    try{
        let d = new Date(isoTime);
        lTime = dateFormat("yyyy-MM-dd HH:mm:ss", d);
    }catch (e) {
        lTime = isoTime;
    }
    return lTime;
}
