;(function() {

    $('#btn-evcloud-list').click(function () {
        self.location.href = "/evcloud/list/";
    })

    $form_create = $('#form-create');
    

    function getFormJson(form_node) {
        var o = {};
        var a = form_node.serializeArray();
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


    $form_create.submit(function (event) {
        event.preventDefault();
        swal({
            title: '是否创建?',
            text: "你将会创建一台新的虚拟机",
            type: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#3085d6',
            cancelButtonColor: '#d33',
            confirmButtonText: '新建',
            preConfirm: () => {
                let data = getFormJson($form_create);
                //console.log(data);
                let url = '/evcloud/add/';
                return $.ajax({
                    url: url,
                    type: 'post',
                    data: data,
                    timeout: 200000,
                    success: (result) => {
                        if (result.code === 200){
                            return result;
                        }else{
                            swal.showValidationMessage(
                            `Request failed: ${result.error_text}`
                            );
                        }
                    },
                    error: (error) => {
                        swal.showValidationMessage(
                            `Ajax request failed! `
                        );
                    },
                    headers: {'X-Requested-With': 'XMLHttpRequest'},//django判断是否是异步请求时需要此响应头
                    clearForm: false,//禁止清除表单
                    resetForm: false //禁止重置表单
                })
            },
        }).then(
            (result) => {
                if (result.value) {
                    swal({
                        title: `创建成功`,
                    }).then(() => {
                        // location.reload(true);// 刷新当前页面
                        self.location.href = "/evcloud/list/";// 重定向
                    } )
                }
             },
            (error) => {
                swal(`Request failed:发生错误，创建失败！`);
            }
        )
    })

})();



