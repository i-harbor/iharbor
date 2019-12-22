;(function () {

    //API域名
    let DOMAIN_NAME = get_domain_url();

    // 获取API域名
    function get_api_domain_name(){
        return DOMAIN_NAME;
    }

    // 构建带域名url
    function build_url_with_domain_name(url){
        let domain = get_api_domain_name();
        domain = domain.rightStrip('/');
        if(!url.startsWith('/'))
            url = '/' + url;
        return domain + url;
    }

        //
    //所有ajax的请求的全局设置
    //
    $.ajaxSettings.beforeSend = function(xhr, settings){
        var csrftoken = getCookie('csrftoken');
        if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
            xhr.setRequestHeader("X-CSRFToken", csrftoken);
        }
    };

    //
    // FTP密码双击修改事件
    //
    $("#content-display-div").on("dblclick", '.vpn-password', function (e) {
        e.preventDefault();
        let remarks = $(this).children('.vpn-password-value');
        let old_html = remarks.text();
        old_html = old_html.replace(/(^\s*) | (\s*$)/g,'');

        //如果已经双击过，正在编辑中
        if(remarks.attr('data-in-edit') === 'true'){
            return;
        }
        // 标记正在编辑中
        remarks.attr('data-in-edit', 'true');
        //创建新的input元素，初始内容为原备注信息
        var newobj = document.createElement('input');
        newobj.type = 'text';
        newobj.value = old_html;
        //设置该标签的子节点为空
        remarks.empty();
        remarks.append(newobj);
        newobj.setSelectionRange(0, old_html.length);
        //设置获得光标
        newobj.focus();
        //为新增元素添加光标离开事件
        newobj.onblur = function () {
            remarks.attr('data-in-edit', '');
            remarks.empty();
            let input_text = this.value;
            // 如果输入内容修改了
            if (input_text && (input_text !== old_html)){
                if (input_text.length < 6){
                    show_warning_dialog('密码长度不得小于6个字符', 'warning');
                    remarks.append(old_html);
                    return;
                }
                // 请求修改ftp密码
                password = input_text;
                let url = build_url_with_domain_name('api/v1/vpn/');
                let ret = vpn_password_ajax(url, password);
                if(ret.ok){
                    show_warning_dialog("修改密码成功", "success");
                }else{
                    // 修改失败，显示原内容
                    input_text = old_html;
                    show_warning_dialog('修改密码失败,' + ret.msg, 'error');
                }
            }
            remarks.append(input_text);
        };
    });

    function vpn_password_ajax(url, password) {
        let ret = {ok:false, msg:''};

        $.ajax({
            url: url,
            type: "POST",
            data: {password: password},
            content_type: "application/json",
            timeout: 5000,
            async: false,
            success: function (res) {
                if(res.code === 201){
                    ret.ok = true;
                }
            },
            error: function(xhr, status){
                if (status === 'timeout') {// 判断超时后 执行
                    ret.msg = "请求超时";
                }else if (xhr.responseJSON.hasOwnProperty('code_text')){
                    ret.msg = xhr.responseJSON.code_text;
                }else{
                    ret.msg = '请求失败';
                }
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},//django判断是否是异步请求时需要此响应头
        });

        return ret;
    }

})();