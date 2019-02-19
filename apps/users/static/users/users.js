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
        <tr>
            <th>{{ token.created }}</th>
            <th>
                <input type="password"  class="col-sm-10" value="{{ token.key }}" style="border: 0px;outline:none;">
                <span class="btn btn-default secret-key-display glyphicon glyphicon-eye-open"></span>
            </th>
            <th><span class="btn btn-danger" id="btn-new-token"><span class="glyphicon glyphicon-refresh"></span>创建新密钥</span></th>
        </tr>
    `);
    
    //
    // 获取和新建suth token,并渲染
    //@type: 'get':获取； 'put':新建
    function get_or_refresh_auth_token(type, render) {
        $.ajax({
                url: build_url_with_domain_name('/api/v1/auth-token/'),
                type: type,
                data: {},
                timeout: 200000,
                success: function (result, status, xhr) {
                    let $btn_tr = $("#btn-new-token").parent();
                    let $token_tr = $btn_tr.prev();
                    $token_tr.children('input').val(result.token.key);
                    $token_tr.prev(2).text(result.token.created);
                },
                error:function(xhr, status, errortext){
                    show_warning_dialog('刷新token失败');
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
            $(this).removeClass("glyphicon glyphicon-eye-open");
            $(this).addClass("glyphicon glyphicon-eye-close");
        }else{
            secret.attr("type", "password");
            $(this).removeClass("glyphicon glyphicon-eye-close");
            $(this).addClass("glyphicon glyphicon-eye-open");
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
            title: '确认要停用此访问密钥吗？',
            text: '请确认',
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'PATCH',
                    timeout: 200000,
                    success: function (result, status, xhr) {
                        btn.text('启用');
                        btn.removeClass('btn-stop-auth-key');
                        btn.addClass('btn-active-auth-key');
                        let state = btn.parent().prev();
                        state.text('停用');
                        state.removeClass('text-success');
                        state.addClass('text-warning');
                        show_warning_dialog('已成功停用访问密钥', 'success');
                    },
                    error: function (xhr, status, errortext) {
                        show_warning_dialog('起用访问密钥失败');
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
            title: '确认要启用此访问密钥吗？',
            text: '请确认',
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'PATCH',
                    timeout: 200000,
                    success: function (result, status, xhr) {
                        btn.text('停用');
                        btn.removeClass('btn-active-auth-key');
                        btn.addClass('btn-stop-auth-key');
                        let state = btn.parent().prev();
                        state.text('使用中');
                        state.removeClass('text-warning');
                        state.addClass('text-success');
                        show_warning_dialog('已成功起用访问密钥', 'success');
                    },
                    error: function (xhr, status, errortext) {
                        show_warning_dialog('起用访问密钥失败');
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
            title: '确认要删除此访问密钥吗？',
            text: '请确认',
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'DELETE',
                    timeout: 200000,
                    success: function (result, status, xhr) {
                        tr_key.remove();
                        show_warning_dialog('已成功删除访问密钥', 'success');
                    },
                    error: function (xhr, status, errortext) {
                        show_warning_dialog('删除访问密钥失败');
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
            <td>{{ key.create_time }}</td>
            <td>{{ key.access_key }}</td>
            <td class="col-sm-5">
                <input title="secret_key" class="secret-key col-sm-10" readonly type="password" value="{{ key.secret_key }}" style="border: 0px;outline:none;">
                <span class="btn btn-default secret-key-display glyphicon glyphicon-eye-open"><span class=""></span></span>
            </td>
            {{ if key.state }}
                <td class="text-success">使用中</td>
            {{else if !key.state }}
                <td class="text-warning">停用</td>
            {{/if}}
            <td>
                {{ if key.state }}
                    <span class="btn btn-info btn-stop-auth-key">停用</span>
                {{else if !key.state }}
                    <span class="btn btn-info btn-active-auth-key">启用</span>
                {{/if}}
                <span class="btn btn-danger btn-remove-auth-key"><span class="glyphicon glyphicon-remove"></span>删除</span>
            </td>
        </tr>
    `);

    //
    // 创建访问密钥
    //
    $("#btn-create-auth-key").on('click', function () {
        let url = get_api_domain_name() + get_auth_key_api_base();
        show_confirm_dialog({
            title: '确认要创建新的访问密钥吗？',
            text: '请确认',
            ok_todo: function () {
                $.ajax({
                    url: url,
                    type: 'POST',
                    timeout: 200000,
                    success: function (data, status, xhr) {
                        let html_str = render_auth_key_table_item(data);
                        $('#auth-key-table').children('tbody').children().first().after(html_str);
                        show_warning_dialog('已成功创建新的访问密钥', 'success');
                    },
                    error: function (error, status, errortext) {
                        let msg;
                        try {
                            msg = error.responseJSON.code_text;
                        }
                        catch (e) {
                            msg = error.statusText;
                        }
                        show_warning_dialog('创建访问密钥失败:' + msg);
                    },
                });
            },
        });
    });

})();
