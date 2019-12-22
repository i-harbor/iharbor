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


     var OLD_VPN_PASSWORD;
    $("#content-display-div").on("click", '.vpn-password-edit', function (e) {
        e.preventDefault();

        let input = $(this).prev();
        OLD_VPN_PASSWORD = input.val();
        input.removeAttr("readonly");
        input.after(`<span class="glyphicon glyphicon-floppy-saved vpn-password-save"></span>`);
        $(this).remove();
    });

    $("#content-display-div").on("click", '.vpn-password-save', function (e) {
        e.preventDefault();

        let input = $(this).prev();
        password = input.val();
        if (password.length < 6){
            show_warning_dialog('密码长度不得小于6个字符', 'warning');
            return;
        }
        if (password !== OLD_VPN_PASSWORD){
            // 请求修改ftp密码
            let url = build_url_with_domain_name('api/v1/vpn/');
            let ret = vpn_password_ajax(url, password);
            if(ret.ok){
                show_warning_dialog("修改密码成功", "success");
            }else{
                // 修改失败，显示原内容
                input.val(OLD_VPN_PASSWORD);
                show_warning_dialog('修改密码失败,' + ret.msg, 'error');
            }
        }
        input.attr("readonly", "readonly");
        input.after(`<span class="glyphicon glyphicon-edit vpn-password-edit"></span>`);
        $(this).remove();
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

    //内容星号显示或隐藏
    $("#content-display-div").on("mouseover", '.vpn-password', function (e) {
        e.preventDefault();
        $(this).children('.vpn-password-value').attr("type", "text");
    });
    $("#content-display-div").on("mouseout", '.vpn-password', function (e) {
        e.preventDefault();
        $(this).children('.vpn-password-value').attr("type", "password");
    });

})();