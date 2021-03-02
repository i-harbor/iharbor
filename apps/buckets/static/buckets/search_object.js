;(function () {
    //API域名
    let DOMAIN_NAME = get_domain_url(); //'http://10.0.86.213:8000/';

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

    function get_err_msg_or_default(xhr, default_msg) {
        let msg = default_msg;
        try {
            let data = xhr.responseJSON;
            if (data.hasOwnProperty('code_text')) {
                msg = default_msg + ';' + data.code_text;
            }
        } catch (e) {

        }
        return msg;
    }

    // 翻译字符串，包装django的gettext
    function getTransText(str) {
        try {
            return gettext(str);
        } catch (e) {
        }
        return str;
    }

    function transInterpolate(fmt, obj, named) {
        try {
            return interpolate(fmt, obj, named)
        } catch (e) {
        }

        if (named) {
            return fmt.replace(/%\(\w+\)s/g, function (match) {
                return String(obj[match.slice(2, -2)])
            });
        } else {
            return fmt.replace(/%s/g, function (match) {
                return String(obj.shift())
            });
        }
    }

    //art-template渲染模板注册过滤器
    template.defaults.imports.sizeFormat = sizeFormat;
    template.defaults.imports.isoTimeToLocal = isoTimeToLocal;
    template.defaults.imports.getTransText = getTransText;
    template.defaults.imports.interpolate = transInterpolate;

    //存储桶文件列表视图渲染模板
    let render_search_objects_table = template.compile(`
        <div class="container-fluid">
            <table class="table table-hover" id="bucket-files-table">
                <thead class="thead-light">
                <tr class="bg-light">
                    <th><input type="checkbox" data-check-target=".item-checkbox" /></th>
                    <th>{{$imports.getTransText('名称')}}</th>
                    <th>{{$imports.getTransText('上传时间')}}</th>
                    <th>{{$imports.getTransText('大小')}}</th>
                    <th>{{$imports.getTransText('权限')}}</th>
                </tr>
                </thead>
                {{each files}}
                <tbody>
                    <tr class="bucket-files-table-item">
                        <td><input type="checkbox" class="item-checkbox" value=""></td>
                        <!--文件-->
                        {{ if $value.fod }}
                            <td>
                                <i class="fa fa-file"></i>
                                <a href="#" id="bucket-files-item-enter-file" download_url="{{$value.download_url}}">{{ $value.na }}</a>
                            </td>
                            <td>{{ $imports.isoTimeToLocal($value.ult) }}</td>
                            <td>{{ $imports.sizeFormat($value.si, "B") }}</td>
                        {{/if}}
                        {{ if !$value.fod }}
                            <td>
                                <i class="fa fa-folder"></i>
                                <a href="#" id="bucket-files-item-enter-dir"><strong>{{ $value.na }}</strong></a>
                            </td>
                            <td>{{ $imports.isoTimeToLocal($value.ult) }}</td>
                            <td>--</td>
                        {{/if}}
                        <td id="id-access-perms">{{ $value.access_permission}}</td>
                    </tr>
                </tbody>
                {{/each}}
                <tr><td colspan="6"><%= $imports.interpolate($imports.getTransText('共 %s 个项目'), [count]) %></td></tr>
            </table>
        </div>
        {{if (previous || next)}}
        <div class="container-fluid">
            <div class="row">
                <nav aria-label="Page navigation col-6" style="margin:0;">
                  <ul class="pagination">
                    {{if previous}}
                        <li class="page-item"><a class="page-link" id="page_previous_bucket_files" href="{{previous}}"><span aria-hidden="true">&laquo;</span>{{$imports.getTransText('上页')}}</a></li>
                    {{/if}}
                    {{if !previous}}
                        <li class="page-item disabled"><a class="page-link"><span aria-hidden="true">&laquo;</span>{{$imports.getTransText('上页')}}</a></li>
                    {{/if}}                
                    {{if page}}
                        <li class="page-item disabled"><span class="page-link"><%= $imports.interpolate($imports.getTransText('第%s页 / 共%s页'), [page.current, page.final]) %></span></li>
                    {{/if}}
                    {{if next}}
                        <li class="page-item"><a class="page-link" id="page_next_bucket_files" href="{{next}}">{{$imports.getTransText('下页')}}<span aria-hidden="true">&raquo;</span></a></li>
                    {{/if}}
                    {{if !next}}
                        <li class="page-item disabled"><span class="page-link">{{$imports.getTransText('下页')}}<span aria-hidden="true">&raquo;</span></span></li>
                    {{/if}}
                  </ul>
                </nav>
            </div>
        </div>
        {{/if}}
    `);

    $("#button-search-objects").click(function (e){
        e.preventDefault();
        let bucket = $("#id-search-bucket").val();
        if (!bucket){
            show_warning_dialog(getTransText('请选择要搜索的存储桶'))
            return
        }
        let search = $("#id-search-keyword").val();
        if (!search){
            show_warning_dialog(getTransText('请输入要搜索的对象名称'))
            return
        }
        let params = encode_params({bucket: bucket, search: search, limit: 2})
        let url = build_url_with_domain_name('api/v1/search/object/?' + params)
        get_search_objects_and_render(url, render_search_objects_table)
    })

    function get_search_objects_and_render(url, render) {
        Swal.showLoading();
        $.ajax({
            url: url,
            timeout: 20000,
            success: function (data, status, xhr) {
                Swal.close();
                if (xhr.status === 200) {
                    let html = render(data);
                    let content_display_div = $("#search-object-display");
                    content_display_div.empty();
                    content_display_div.append(html);
                } else {
                    show_warning_dialog(getTransText('好像出问题了，跑丢了') + '( T__T ) …', 'error');
                }
            },
            error: function (xhr, errtype, error) {
                Swal.close();
                if (errtype === 'timeout') {
                    show_warning_dialog(getTransText('请求超时'), 'error');
                } else {
                    show_warning_dialog(get_err_msg_or_default(xhr, getTransText('好像出问题了，跑丢了')) + '( T__T ) …', 'error');
                }
            }
        });
    }

    // 文件夹、文件对象列表上一页Previous点击事件
    $("#search-object-display").on("click", '#page_previous_bucket_files', function (e) {
        e.preventDefault();
        let url = $(this).attr('href');
        get_search_objects_and_render(url, render_search_objects_table)
    });

    // 文件夹、文件对象列表下一页Next点击事件
    $("#search-object-display").on("click", '#page_next_bucket_files', function (e) {
        e.preventDefault();
        let url = $(this).attr('href');
        get_search_objects_and_render(url, render_search_objects_table)
    });

})();
