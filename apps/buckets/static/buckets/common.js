
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
        title: title,
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


