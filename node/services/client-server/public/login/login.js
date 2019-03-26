$('.login-form').submit(function(event) {
    event.preventDefault();
    postData('login', {userName: $('#username').val(), password: $('#password').val()});
});

function postData(url, data) {
    return $.ajax({
        url: url,
        type: "POST",
        data: data,
        xhrFields: {
            withCredentials: true
        },
    })
}

function setCookie(cname, cvalue, exp) {
    var d = new Date();
    d.setTime(d.getTime() + (exp));
    var expires = "expires="+ d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}