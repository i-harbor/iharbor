;(function () {
        //API域名
    let DOMAIN_NAME = ''; //'http://10.0.86.213:8000/';

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
    // auth token 新建按钮点击事件
    //
    $("#btn-new-token").on("click", function () {
        show_confirm_dialog({
            title: "确定刷新token吗？",
            text: "此操作会重新创建一个token，旧token将失效！",
            ok_todo: function () {
                get_or_refresh_auth_token('put', render_auth_token_table);
            },
        });
    });

    //
    // token渲染模板
    //
    let render_auth_token_table = template.compile(`
        <tr class="bg-info">
            <th>创建时间</th>
            <th>Token</th>
            <th></th>
        </tr>
        <tr>
            <th>{{ token.created }}</th>
            <th>{{ token.key }}</th>
            <th><span class="btn btn-danger" id="btn-new-token"><span class="glyphicon glyphicon-refresh"></span>创建新密钥</span></th>
        </tr>
    `);
    
    //
    // 获取和新建suth token,并渲染
    //@type: 'get':获取； 'put':新建
    function get_or_refresh_auth_token(type) {
        $.ajax({
                url: build_url_with_domain_name('/api/v1/auth-token/'),
                type: type,
                data: {},
                timeout: 200000,
                success: function (result, status, xhr) {
                    let $btn_tr = $("#btn-new-token").parent();
                    let $token_tr = $btn_tr.prev();
                    $token_tr.text(result.token.key);
                    $token_tr.prev(2).text(result.token.created);
                },
                error:function(xhr, status, errortext){
                    show_warning_dialog('刷新token失败');
                },
            });
    }

})();
