'use strict';

// region bg colors
const colors = [
    "bg-navy",
    "bg-blue",
    "bg-aqua",
    "bg-teal",
    "bg-olive",
    "bg-green",
    "bg-lime",
    "bg-yellow",
    "bg-orange",
    "bg-red",
    "bg-maroon",
    "bg-fuchsia",
    "bg-purple",
    "bg-black",
    "bg-silver",
    "bg-gray"
];

// synchronize with server/event.go
const eventType = {
    SPLIT : 1,
    TRANSFER_LEADER: 2,
    ADD_REPLICA : 3,
    REMOVE_REPLICA : 4
};

const eventStatus = {
    START : 1,
    FINISHED : 2
};

var dashboardApp = angular.module('dashboardApp', ['ngAnimate']);

const eventRowTmpl = `
<div class="jumbotron" ng-model="event">
    <div class="jumbotron-contents">
        <div class="row">
            <div class="col-sm-1 log-img">
                <i class="fa fa-scissors fa-2x" ng-if="event.code == eventType.SPLIT"></i>

                <i class="fa fa-exchange fa-2x" ng-if="event.code == eventType.TRANSFER_LEADER && event.status != eventStatus.FINISHED"></i>
                <i class="fa fa-check fa-2x fa-green" ng-if="event.code == eventType.TRANSFER_LEADER && event.status == eventStatus.FINISHED"></i>

                <i class="fa fa-refresh fa-2x" ng-if="event.code == eventType.ADD_REPLICA && event.status != eventStatus.FINISHED"></i>
                <i class="fa fa-check fa-2x fa-green" ng-if="event.code == 3 && event.status == eventStatus.FINISHED"></i>

                <i class="fa fa-trash fa-2x" ng-if="event.code == eventType.REMOVE_REPLICA && event.status != eventStatus.FINISHED "></i>
                <i class="fa fa-check fa-2x fa-green" ng-if="event.code == eventType.REMOVE_REPLICA && event.status == eventStatus.FINISHED"></i>

            </div>
            <div class="col-md-10 log-msg">

                <!-- split message -->
                <div ng-if="event.code == eventType.SPLIT">
                    Split
                    <span class="label {{ colors[event.split_event.region % colors.length] }}">Region {{ event.split_event.region  }}</span> into
                    <span class="label {{ colors[event.split_event.left % colors.length] }}">Region {{ event.split_event.left }}</span> and
                    <span class="label {{ colors[event.split_event.right % colors.length] }}">Region {{ event.split_event.right }}</span>
                </div>

                <!-- leader transfer message -->
                <div ng-if="event.code == eventType.TRANSFER_LEADER">
                    Transfer leadership of
                    <span class="label {{ colors[event.transfer_leader_event.region % colors.length] }}">Region {{ event.transfer_leader_event.region }}</span> from 
                    <b>Node {{ event.transfer_leader_event.store_from }}</b> to <b> Node {{ event.transfer_leader_event.store_to }}</b>
                    <label ng-if="event.status == eventStatus.FINISHED" class="label label-success">Finished</label>
                </div>

                <!-- add replica message -->
                <div ng-if="event.code == eventType.ADD_REPLICA">
                    Add Replica for <span class="label {{ colors[event.add_replica_event.region % colors.length] }}"> Region {{ event.add_replica_event.region }}</span> 
                    to <b> Node {{ event.add_replica_event.store }}</b>
                    <label ng-if="event.status == eventStatus.FINISHED" class="label label-success">Finished</label>
                </div>

                <!-- remove replica message -->
                <div ng-if="event.code == eventType.REMOVE_REPLICA">
                    Remove Replica for <span class="label {{ colors[event.remove_replica_event.region % colors.length] }}"> Region {{ event.remove_replica_event.region }}</span> 
                    from <b> Node {{ event.remove_replica_event.store }}</b>
                    <label ng-if="event.status == eventStatus.FINISHED" class="label label-success">Finished</label>
                </div>

            </div>
        </div>
    </div>
</div>
`;

dashboardApp.directive('eventRow', function() {
    return {
        restrict: 'AE',
        scope: {
            event: '=',
        },
        replace: 'true',
        template: eventRowTmpl,
        controller: ['$scope', function($scope) {
            $scope.eventType = eventType;
            $scope.eventStatus = eventStatus;
            $scope.colors = colors;
        }]
    };
});

dashboardApp.controller('LogEventController', function LogEventController($scope, $timeout, $http) {

    $scope.logs = [];

    $scope.init = function(wsHost) {
            var ws = new WebSocket("ws://" + wsHost + "/pd/ws");

            ws.onopen = function(evt) {
                console.log("ws onopen");
                $http({
                    method: 'GET',
                    url: 'http://' + wsHost + '/pd/api/v1/feed?offset=0',
                }).then(function(dataResponse) {
                    if (dataResponse.data != null) {
                        console.log(dataResponse.data)
                        $scope.logs = dataResponse.data.reverse();
                    }
                });
            }

            ws.onclose = function(evt) {
                console.log("ws onclose");
                ws = null;
            }

            ws.onmessage = function(evt) {
                console.log(evt.data);
                $scope.$apply(function () {
                    var data = JSON.parse(evt.data);
                    $scope.logs.unshift(data);

                    if ($scope.logs.length > 2000) {
                        $scope.logs.pop();
                    }
                });
            }

            ws.onerror = function(evt) {
                console.log("ws onerror");
                console.log(evt);
            }
    };

});
