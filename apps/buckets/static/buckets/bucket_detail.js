;(function() {
    //API域名
    let DOMAIN_NAME = get_domain_url();
    // 获取API域名
    function get_api_domain_name() {
        return DOMAIN_NAME;
    }

    // 构建带域名url
    function build_url_with_domain_name(url) {
        let domain = get_api_domain_name();
        domain = domain.rightStrip('/');
        if (!url.startsWith('/'))
            url = '/' + url;
        return domain + url;
    }

    // 翻译字符串，包装django的getTransText
    function getTransText(str) {
        try {
            return gettext(str);
        } catch (e) {
        }
        return str;
    }

    //所有ajax的请求的全局设置
    $.ajaxSettings.beforeSend = function (xhr, settings) {
        var csrftoken = getCookie('csrftoken');
        if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
            xhr.setRequestHeader("X-CSRFToken", csrftoken);
        }
    };

    function get_bucket_name() {
        return $("#id-bucket-name").text()
    }

    function get_message(xhr, orDefault, key=null){
        if (!key){
            key = 'message';
        }
        try {
            let msg = xhr.responseJSON[key];
            if (msg) {
                return msg;
            }else{
                return orDefault;
            }
        }catch (e) {
            return orDefault;
        }
    }

    $(".bucket-token-create").click(function (e) {
            e.preventDefault();
            show_confirm_dialog({
                title: getTransText('是否创建新的token？'),
                text: getTransText('每个存储桶只能创建2个token'),
                ok_todo: function () {
                    bucketTokenCreate();
                }
            });
        }
    );

    async function bucketTokenCreate() {
        const {value: permission} = await Swal.fire({
            title: getTransText('创建存储桶Token'),
            input: 'select',
            inputOptions: {
                readonly: getTransText('只读'),
                readwrite: getTransText('读写')
            },
            inputPlaceholder: getTransText('选择访问权限'),
            showCancelButton: true,
            inputValidator: (value) => {
                return new Promise((resolve) => {
                    if (value) {
                        resolve()
                    } else {
                        resolve(getTransText('请选择一个选项'))
                    }
                })
            }
        });

        if (permission) {
            let bucket = get_bucket_name();
            let query_str = $.param({"by-name": true, "permission": permission}, true);
            let url = build_url_with_domain_name(`api/v1/buckets/${bucket}/token/create/?${query_str}`);
            $.ajax(url, {
                type: 'POST',
                success: function (data, status_text, xhr) {
                    let delete_str = getTransText('删除');
                    let created_time = isoTimeToLocal(data.created);
                    let html = `
                        <tr>
                            <td class="mouse-hover">
                              <span class="mouse-hover-show">${data.key}</span>
                              <span class="mouse-hover-no-show">******</span>
                            </td>
                            <td>${data.permission}</td>
                            <td>${created_time}</td>
                            <td>
                                <button class="btn btn-danger btn-sm bucket-token-delete">${delete_str}</button>
                            </td>
                        </tr>`;
                    $("#table-bucket-token").children('tbody').append(html);
                },
                error: function (xhr, errType, err) {
                    let msg = get_message(xhr, getTransText('创建失败'));
                    Swal.fire({
                        title: msg,
                        icon: 'warning'
                    });
                },
            });
        }
    }

    $("#table-bucket-token").on('click', '.bucket-token-delete', function (e) {
        e.preventDefault();
        let dom_tr = $(this).parents('tr:first');
        let token = dom_tr.children("td:first").children("span:first").text();
        let url = build_url_with_domain_name(`api/v1/bucket-token/${token}/`);
        show_confirm_dialog({
            title: getTransText('请确认是否删除？'),
            ok_todo: function () {
                $.ajax(url, {
                    type: "DELETE",
                    success: function (data, status_text, xhr) {
                        dom_tr.remove();
                        show_auto_close_warning_dialog(getTransText('删除成功'), 'success')
                    },
                    error: function (xhr, errType, err) {
                        let msg = get_message(xhr, getTransText('删除失败'));
                        show_warning_dialog(msg);
                    },
                })
            }
        });
    });

})();