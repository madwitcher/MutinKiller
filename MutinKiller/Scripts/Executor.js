var group_ids = [];
var user_ids = [];
var timeout = 1000;

function GetFriends() {
    VK.api('friends.get', {}, function (data) {
        if (data.response) {
            $("#divMessage").append("<br/>Friends count: " + data.response.items.length);
            user_ids = data.response.items;
            setTimeout(function () { GetGroupsFor(user_ids.pop()); }, timeout);
        }
    });
}

function GetGroupsFor(userId) {
    VK.api('groups.get', { user_id: userId }, function (data) {
        if (data.response) {
            $("#divMessage").append("<br/>User ID: " + userId + ", Groups Count: " + data.response.items.length);
            $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            if (data.response.items != undefined && data.response.items.length > 0) {
                group_ids.push(data.response.items);
            }

            if (user_ids.length > 0) {
                setTimeout(function () { GetGroupsFor(user_ids.pop()); }, timeout);
            }
            else {
                DisplayGroupsCount();
            }
        }
        else {
            $("#divMessage").append("<br/>User ID: " + userId + "&nbsp;<span style='color:red'>" + data.error.error_code + ":" + data.error.error_msg + "</span>");
            $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            if (user_ids.length > 0) {
                setTimeout(function () { GetGroupsFor(user_ids.pop()); }, timeout);
            }
            else {
                DisplayGroupsCount();
            }
        }
    });
}

function DisplayGroupsCount() {
    var groupsAlreadyInList = [];

    for (var index in group_ids) {
        for (var innerIndex in group_ids[index]) {
            if (IsNotInList(groupsAlreadyInList, group_ids[index][innerIndex])) {
                groupsAlreadyInList.push(group_ids[index][innerIndex]);
            }
        }
    }

    $("#divMessage").append("<br/>All friends groups count: " + groupsAlreadyInList.length);
    $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
}

function IsNotInList(list, item) {
    var isNotInList = true;

    for (var index in list) {
        if (list[index] == item) {
            inList = false;
            break;
        }
    }

    return isNotInList;
}

function SaveAccessToken() {
    var accessToken = '';
    var queryString = location.href.split("?")[1];
    var queryStringParts = queryString.split("&");
    for (var index in queryStringParts) {
        var keyValueParts = queryStringParts[index].split("=");
        if (keyValueParts[0] == "access_token") {
            accessToken = keyValueParts[1];
            break;
        }
    }
    $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word'>Access token: " + accessToken + "</span>");
    $("#divMessage").append("<br/>Saving access token...");
    $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
    $.ajax({
        type: "GET",
        url: "MutinKiller/Index/SaveAccessToken?accessToken=" + accessToken,
        contentType: "application/json",
        success: function (result) {
            if (result.Failed) {
                $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Access token was not saved. Error: " + result.ErrorMessage + "</span>");
            }
            else {
                $("#divMessage").append("<br/>Access token was saved.");
            }
            $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
        },
        error: function (error) {
            $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Error upon saving: " + error.responseText + "</span>");
            $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
        }
    });
}

function QueueGroup() {
    var dlgButtons = [
            {
                text: "Queue",
                click: function () { QueueGroupComplete(); }
            },
            {
                text: "Cancel",
                click: function () { CloseQueueGroupDialog(); }
            }
            ];
    $("#body").append("<div id='dlgQueueGroup'>Group ID: &nbsp;<input type='text' style='width:150px' id='txtGroupId' /></div>");
    $("#dlgQueueGroup").dialog({
        resizable: false,
        height: 150,
        width: 300,
        zIndex: 10000,
        title: "Queue Group",
        modal: true,
        buttons: dlgButtons,
        close: function () {
            CloseQueueGroupDialog();
        }
    });
}

function QueueGroupComplete() {
    var groupId = $("#txtGroupId").val();

    if (groupId == null || groupId == '' || groupId == undefined || isNaN(groupId) || groupId.match(/\d/g).length != groupId.length || Number(groupId) < 1) {
        alert("Group ID is invalid.");
    }
    else {
        CloseQueueGroupDialog();
        $.ajax({
            type: "GET",
            url: "Index/QueueObject?type=group&id=" + groupId,
            contentType: "application/json",
            success: function (result) {
                if (result.Failed) {
                    $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Group was not queued. Error: " + result.ErrorMessage + "</span>");
                }
                else {
                    $("#divMessage").append("<br/>Group ID '" + groupId + "' was queued.");
                }
                $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            },
            error: function (error) {
                $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Error upon queuing: " + error.responseText + "</span>");
                $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            }
        });
    }
}

function CloseQueueGroupDialog() {
    $("#dialog:ui-dialog").dialog("destroy");
    $($(".ui-dialog")[0]).remove();
    $($("#dlgQueueGroup")[0]).remove();
}

function QueueUser() {
    var dlgButtons = [
            {
                text: "Queue",
                click: function () { QueueUserComplete(); }
            },
            {
                text: "Cancel",
                click: function () { CloseQueueUserDialog(); }
            }
            ];
    $("#body").append("<div id='dlgQueueUser'>User ID: &nbsp;<input type='text' style='width:150px' id='txtUserId' /></div>");
    $("#dlgQueueUser").dialog({
        resizable: false,
        height: 150,
        width: 300,
        zIndex: 10000,
        title: "Queue User",
        modal: true,
        buttons: dlgButtons,
        close: function () {
            CloseQueueUserDialog();
        }
    });
}

function QueueUserComplete() {
    var userId = $("#txtUserId").val();

    if (userId == null || userId == '' || userId == undefined || isNaN(userId) || userId.match(/\d/g).length != userId.length || Number(userId) < 1) {
        alert("User ID is invalid.");
    }
    else {
        CloseQueueUserDialog();
        $.ajax({
            type: "GET",
            url: "MutinKiller/Index/QueueObject?type=user&id=" + userId,
            contentType: "application/json",
            success: function (result) {
                if (result.Failed) {
                    $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>User was not queued. Error: " + result.ErrorMessage + "</span>");
                }
                else {
                    $("#divMessage").append("<br/>User ID '" + userId + "' was queued.");
                }
                $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            },
            error: function (error) {
                $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Error upon queuing: " + error.responseText + "</span>");
                $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            }
        });
    }
}

function CloseQueueUserDialog() {
    $("#dialog:ui-dialog").dialog("destroy");
    $($(".ui-dialog")[0]).remove();
    $($("#dlgQueueUser")[0]).remove();
}

function ShowStats() {
    (waitScreen = new AGW_WaitScreen("#body", "Loading...", "Please wait")).Show();
    $.ajax({
        type: "GET",
        url: "MutinKiller/Index/GetStats",
        contentType: "application/json",
        success: function (result) {
            waitScreen.Close();
            if (result.Failed) {
                $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Statistic is not available. Error: " + result.ErrorMessage + "</span>");
                $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            }
            else {
                ShowStatsDialog(result.Stats);
            }
        },
        error: function (error) {
            waitScreen.Close();
            $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Error upon getting stats: " + error.responseText + "</span>");
            $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
        }
    });
}

function ShowStatsDialog(stats) {
    var dlgButtons = [
            {
                text: "Refresh",
                click: function () { CloseStatsDialog(); ShowStats(); }
            },
            {
                text: "Close",
                click: function () { CloseStatsDialog(); }
            }
            ];
    $("#body").append(GetShowStatsDialog(stats));
    $("#dlgStats").dialog({
        resizable: false,
        height: 490,
        width: 580,
        zIndex: 10000,
        title: "Killer statistics",
        modal: true,
        buttons: dlgButtons,
        close: function () {
            CloseStatsDialog();
        },
        open: function (event, ui) {
            $("#divLog")[0].scrollTop = $("#divLog")[0].scrollHeight;
        }
    });
}

function GetShowStatsDialog(stats) {
    var html = "<div style='height:100%;width:100%' id='dlgStats'>";
    //html += "Executor status: " + (stats.IsExecutorAlive ? "<span style='color:Green;font-style:bold;'>ALIVE</span>" : "<span style='color:Red;font-style:bold;'>DEAD</span>") + "<br/>";
    html += "Queue Length: " + stats.QueueLength + "<br/>";
    html += "Groups in DB: " + stats.GroupsCount + "<br/>";
    html += "Users in DB: " + stats.UsersCount + "<br/><br/>";
    html += "<center>Log:</center><br/><div style='width:100%;height:260px;overflow-x:hidden;overflow-y:auto' id='divLog'><table style='width:100%'>";

    for (var index in stats.Log) {
        html += "<tr><td style='color:grey'>" + stats.Log[index].Key + "</td><td>" + stats.Log[index].Value + "</td></tr>";
    }

    html += "</table></div></div>";

    return html;
}

function CloseStatsDialog() {
    $("#dialog:ui-dialog").dialog("destroy");
    $($(".ui-dialog")[0]).remove();
    $($("#dlgStats")[0]).remove();
}

function ShowGraphs() {
    (waitScreen = new AGW_WaitScreen("#body", "Loading...", "Please wait")).Show();
    $.ajax({
        type: "GET",
        url: "MutinKiller/Index/GetGraphs",
        contentType: "application/json",
        success: function (result) {
            if (result.Failed) {
                waitScreen.Close();
                $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Graphs are not available. Error: " + result.ErrorMessage + "</span>");
                $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
            }
            else {
                ShowGraphsDialog(result.Graphs);
            }
        },
        error: function (error) {
            waitScreen.Close();
            $("#divMessage").append("<br/><span style='width:100%;display:block;word-wrap:break-word;color:red'>Error upon getting graphs: " + error.responseText + "</span>");
            $("#divMessage")[0].scrollTop = $("#divMessage")[0].scrollHeight;
        }
    });
}

function ShowGraphsDialog(graphs) {
    var dlgButtons = [
            {
                text: "Refresh",
                click: function () { CloseGraphsDialog(); ShowGraphs(); }
            },
            {
                text: "Close",
                click: function () { CloseGraphsDialog(); }
            }
            ];
    $("#body").append(GetShowGraphsDialog());

    InitTabsAndGraphs(graphs);
    $("#dlgGraphs").dialog({
        resizable: false,
        height: 490,
        width: 580,
        zIndex: 10000,
        title: "Killer Graphs",
        modal: true,
        buttons: dlgButtons,
        close: function () {
            CloseGraphsDialog();
        }
    });
}

function InitTabsAndGraphs(graphs) {
    //$("#tabs").tabs();
    //$("#tabs").removeClass("ui-widget-header").addClass("ui-widget-header-withoutimage");
    for (var index in graphs) {
        var colors = [];

        for (var i = 0; i < 15; i++) {
            colors.push("#" + (getRand()).toString(16) + (getRand()).toString(16) + (getRand()).toString(16));
        }

        InitSingleGraph(graphs[index], colors);
    }

    waitScreen.Close();
}

function getRand() {
    return Math.floor(Math.random() * (235 - 20 + 1)) + 20;
}


function InitSingleGraph(graph, colors) {

    var r = Raphael(graph.Tab);
    var pie = r.piechart(160, 160, 130, graph.Points, { legend: graph.Legend, colors: colors });

    r.text(350, 20, graph.Header).attr({ font: "20px sans-serif" });
    pie.hover(function () {
        this.sector.stop();
        this.sector.scale(1.1, 1.1, this.cx, this.cy);

        if (this.label) {
            this.label[0].stop();
            this.label[0].attr({ r: 7.5 });
            this.label[1].attr({ "font-weight": 800 });
        }
    }, function () {
        this.sector.animate({ transform: 's1 1 ' + this.cx + ' ' + this.cy }, 500, "bounce");

        if (this.label) {
            this.label[0].animate({ r: 5 }, 500, "bounce");
            this.label[1].attr({ "font-weight": 400 });
        }
    });
}

function GetShowGraphsDialog() {
    var html = "<div style='height:100%;width:100%' id='dlgGraphs'>";

    //html += '<div id="tabs" style="height:99%;width:100%"><ul><li><a href="#tabs-1">Platforms</a></li><li><a href="#tabs-2">Sex</a></li><li><a href="#tabs-3">Countries</a></li><li><a href="#tabs-4">Cities</a></li><li><a href="#tabs-5">Man Names</a></li><li><a href="#tabs-6">Woman Names</a></li></ul>';
    for (var i = 1; i <= 6; i++) {
        html += "<div id='tabs-" + i.toString() + "' style='height:400px;width:500px'></div>";
    }
    html += "</div>";
    return html;
}

function CloseGraphsDialog() {
    $("#dialog:ui-dialog").dialog("destroy");
    $($(".ui-dialog")[0]).remove();
    $($("#dlgGraphs")[0]).remove();
}


function AGW_WaitScreen(containerID, message, titleString) {
    this.containerID = containerID;
    this.message = message;
    this.titleString = titleString == undefined ? "Please wait" : titleString;
}

AGW_WaitScreen.prototype.Show = function () {
    $(this.containerID).append("<div id='divWaitScreen' style='width:100%;height:100%;'><table style='width:100%;height:100%'><tr><td style='width:200px;text-align:center'><span style='width:100%;height:100%;text-align:center;vertical-align:middle' id='spanWaitScreenMessage'>" + this.message + "</span></td></tr></table></div>");
    $("#divWaitScreen").dialog({
        resizable: false,
        height: 120,
        width: 300,
        zIndex: 20000,
        title: this.titleString,
        modal: true,
        closeOnEscape: false,
        open: function (event, ui) {
            $(this).closest(".ui-dialog").find(".ui-dialog-titlebar-close").hide();
        }
    });
}

AGW_WaitScreen.prototype.SetMessage = function (message) {
    $("#spanWaitScreenMessage").html(message);
}

AGW_WaitScreen.prototype.Close = function () {
    //work-around for jquery dialog (reopening issue)
    //$("#dialog:ui-dialog").dialog("destroy");
    //$($(".ui-dialog")[0]).remove();
    $($("#divWaitScreen")[0]).remove();
}
    