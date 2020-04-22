;(function () {
    //API域名
    let DOMAIN_NAME = get_domain_url(); //'http://10.0.86.213:8000/';

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
    function get_auth_key_api_base(){
        return 'api/v1/auth-key/';
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
            title: getTransText("确定刷新token吗？"),
            text: getTransText("此操作会重新创建一个token，旧token将失效"),
            ok_todo: function () {
                get_or_refresh_auth_token('put');
            },
        });
    });

    // 翻译字符串，包装django的gettext
    function getTransText(str){
        try{
            return gettext(str);
        }catch (e) {}
        return str;
    }

    template.defaults.imports.isoTimeToLocal = isoTimeToLocal;
    template.defaults.imports.getTransText = getTransText;
    
    //
    // 获取和新建auth token,并渲染
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
                    $token_tr.children('input').val(result.token.key);
                    $token_tr.prev(2).text(isoTimeToLocal(result.token.created));
                },
                error:function(xhr, status, errortext){
                    show_warning_dialog(getTransText('刷新token失败'));
                },
            });
    }
    
    //
    //secret_key内容星号显示或隐藏点击事件
    //
    $("#security-nav-list").on('click', '.secret-key-display', function () {
        let secret = $(this).siblings(":input");
        if (secret.attr("type") === "password"){
            secret.attr("type", "text");
            $(this).empty();
            $(this).append('<i class="fa fa-eye-slash"></i>')
        }else{
            secret.attr("type", "password");
            $(this).empty();
            $(this).append('<i class="fa fa-eye"></i>')
        }
    });

    //
    // 停用访问密钥
    //
    $("#auth-key-table").on('click', '.btn-stop-auth-key',function () {
        let btn = $(this);
        let access_key_node = btn.parents('tr').children().eq(1);
        let access_key = access_key_node.text();
        let url = get_api_domain_name() + get_auth_key_api_base() + access_key + '/?active=false';
        show_confirm_dialog({
            title: getTransText('确认要停用此访问密钥吗？'),
            text: getTransText('请确认'),
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'PATCH',
                    timeout: 200000,
                    success: function (result, status, xhr) {
                        btn.text(getTransText('启用'));
                        btn.removeClass('btn-stop-auth-key');
                        btn.addClass('btn-active-auth-key');
                        let state = btn.parent().prev();
                        state.text(getTransText('已停用'));
                        state.removeClass('text-success');
                        state.addClass('text-warning');
                        show_warning_dialog(getTransText('已成功停用访问密钥'), 'success');
                    },
                    error: function (xhr, status, errortext) {
                        show_warning_dialog(getTransText('起用访问密钥失败'));
                    },
                });
            },
        });
    });


    //
    // 起用访问密钥
    //
    $("#auth-key-table").on('click', '.btn-active-auth-key', function () {
        let btn = $(this);
        let access_key_node = btn.parents('tr').children().eq(1);
        let access_key = access_key_node.text();
        let url = get_api_domain_name() + get_auth_key_api_base() + access_key + '/?active=true';
        show_confirm_dialog({
            title: getTransText('确认要启用此访问密钥吗'),
            text: getTransText('请确认'),
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'PATCH',
                    timeout: 200000,
                    success: function (result, status, xhr) {
                        btn.text(getTransText('停用'));
                        btn.removeClass('btn-active-auth-key');
                        btn.addClass('btn-stop-auth-key');
                        let state = btn.parent().prev();
                        state.text(getTransText('使用中'));
                        state.removeClass('text-warning');
                        state.addClass('text-success');
                        show_warning_dialog(getTransText('已成功起用访问密钥'), 'success');
                    },
                    error: function (xhr, status, errortext) {
                        show_warning_dialog(getTransText('起用访问密钥失败'));
                    },
                });
            },
        });
    });

    //
    // 删除访问密钥
    //
    $("#auth-key-table").on('click', '.btn-remove-auth-key', function () {
        let btn = $(this);
        let tr_key = btn.parents('tr');
        let access_key_node = tr_key.children().eq(1);
        let access_key = access_key_node.text();
        let url = get_api_domain_name() + get_auth_key_api_base() + access_key + '/';
        show_confirm_dialog({
            title: getTransText('确认要删除此访问密钥吗'),
            text: getTransText('请确认'),
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'DELETE',
                    timeout: 200000,
                    success: function (result, status, xhr) {
                        tr_key.remove();
                        show_warning_dialog(getTransText('已成功删除访问密钥'), 'success');
                    },
                    error: function (xhr, status, errortext) {
                        show_warning_dialog(getTransText('删除访问密钥失败'));
                    },
                });
            },
        });
    });


    //
    // token渲染模板
    //
    let render_auth_key_table_item = template.compile(`
        <tr>
            <td>{{ $imports.isoTimeToLocal(key.create_time) }}</td>
            <td>{{ key.access_key }}</td>
            <td>
                <input title="{{ key.secret_key }}" class="secret-key" readonly type="password" value="{{ key.secret_key }}" style="border: 0px;outline:none;">
                <span class="btn btn-outline-info secret-key-display"><i class="fa fa-eye"></i></span>
            </td>
            {{ if key.state }}
                <td class="text-success">{{$imports.getTransText('使用中')}}</td>
            {{else if !key.state }}
                <td class="text-warning">{{$imports.getTransText('停用')}}</td>
            {{/if}}
            <td>
                {{ if key.state }}
                    <span class="btn btn-info btn-stop-auth-key">{{$imports.getTransText('停用')}}</span>
                {{else if !key.state }}
                    <span class="btn btn-info btn-active-auth-key">{{$imports.getTransText('启用')}}</span>
                {{/if}}
                <span class="btn btn-danger btn-remove-auth-key"><i class="fa fa-trash-alt"></i></span>
            </td>
        </tr>
    `);

    // 创建访问密钥
    $("#btn-create-auth-key").on('click', function () {
        let url = get_api_domain_name() + get_auth_key_api_base();
        show_confirm_dialog({
            title: getTransText('确认要创建新的访问密钥吗'),
            text: getTransText('请确认'),
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'POST',
                    timeout: 200000,
                    success: function (data, status, xhr) {
                        let html_str = render_auth_key_table_item(data);
                        $('#auth-key-table').children('tbody').children().first().after(html_str);
                        show_warning_dialog(getTransText('已成功创建新的访问密钥'), 'success');
                    },
                    error: function (error, status, errortext) {
                        let msg;
                        try {
                            msg = error.responseJSON.code_text;
                        }
                        catch (e) {
                            msg = error.statusText;
                        }
                        show_warning_dialog(getTransText('创建访问密钥失败') + ':' + msg);
                    },
                });
            },
        });
    });

})();
