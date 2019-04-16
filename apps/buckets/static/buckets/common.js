
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
        console.log(result);
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
