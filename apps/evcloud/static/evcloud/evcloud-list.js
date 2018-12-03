;(function() {

    var process = '<div class="progress">' +
                        '<div class="progress-bar progress-bar-striped active" role="progressbar" aria-valuenow="45" aria-valuemin="0" aria-valuemax="100" style="width: 100%">' +
                            '<span class="sr-only">45% Complete</span>' +
                        '</div>' +
                    '</div>'

    $('#btn-evcloud-add').click(function () {
        self.location.href = "/evcloud/add/";
    })
    //获取状态，并且显示
    function get_status(vm_id, click_element) {
        $.ajax({
            url: '/evcloud/list/',
            type: "POST",
            data: {
                "vm_id": vm_id,
                "vm_operate": '6',
                "csrfmiddlewaretoken": $('[name="csrfmiddlewaretoken"]').val(),
            },
            datatype: "json",
            success: function (data) {
                if(data.code === 200){
                    click_element.children('[name="status"]').html(data.e);
                    //click_element.children('[name="status"]').addClass('btn-danger');
                    //click_element.parents('td').siblings('[name="mission"]').html('');
                } else {
                    click_element.children('[name="status"]').html('failed');
                    swal({
                        type: 'error',
                        title: '查询状态失败',
                        text: data.e,
                    })
                }
            },
            error: function () {
                click_element.children('[name="status"]').html('暂停服务');
                swal({
                        type: 'error',
                        title: '暂停服务',
                        text: '当前服务不可用',
                    })
                return 0;
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    }
    //查询虚拟机状态
    $('.vm-line').each(function () {
        get_status(this.id, $(this))
    })

    //启动、关闭、断电、重启、删除
    $('.vm-operate').click(function (event) {
        event.preventDefault();
        let operate_list = ['启动', '关闭', '断电', '重启' ,'删除'];
        click_element = $(this);
        vm_operate = event.target.getAttribute('value');
        vm_id = click_element.parents('tr').attr('id');
        click_element.parents('td').siblings('[name="status"]').html(process);
        click_element.parents('td').siblings('[name="mission"]').html(operate_list[vm_operate]);
        $.ajax({
            url: "/evcloud/list/",
            type: "POST",
            data: {
                "vm_id": vm_id,
                "vm_operate": vm_operate,
                "csrfmiddlewaretoken": $('[name="csrfmiddlewaretoken"]').val(),
            },
            datatype: "json",
            success: function (data) {
                if(data.code === 200) {
                    if (data.status === 'delete') {
                        $("#" + vm_id).remove();
                        return;
                    }
                    get_status(vm_id, click_element.parents('tr'));
                    //click_element.parents('td').siblings('[name="status"]').html(data.status);
                    click_element.parents('td').siblings('[name="mission"]').html('');
                } else {
                    get_status(vm_id, click_element.parents('tr'));
                    click_element.parents('td').siblings('[name="mission"]').html('');
                    swal({
                        type: 'error',
                        title: '操作失败',
                        text: data.e,
                    })
                }
            },
            error: function () {
                get_status(vm_id, click_element.parents('tr'));
                swal({
                        type: 'error',
                        title: '暂停服务',
                        text: '当前服务不可用',
                    })
                click_element.parents('td').siblings('[name="mission"]').html('');
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    })
    //vnc
    $('.vnc-btn').click(function (event) {
        event.preventDefault();
        vm_operate = this.value;
        vm_id = $(this).parents('tr').attr('id');
        $.ajax({
            url: "/evcloud/list/",
            type: "POST",
            data: {
                "vm_id": vm_id,
                "vm_operate": vm_operate,
                "csrfmiddlewaretoken": $('[name="csrfmiddlewaretoken"]').val(),
            },
            datatype: "json",
            async: false,
            success: function (data) {
                if (data.code === 200) {
                    window.open(data.e)
                } else {
                    swal({
                        type: 'error',
                        title: '操作失败',
                        text: data.e,
                    })
                }
            },
            error: function () {
                swal({
                        type: 'error',
                        title: '暂停服务',
                        text: '当前服务不可用',
                    })
            },
            headers: {'X-Requested-With': 'XMLHttpRequest'},
        })
    })


})();



