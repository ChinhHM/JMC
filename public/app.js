angular.module('app', ['ui.bootstrap']);

angular
    .module('app')
    .controller('appCtrl', AppCtrl);

AppCtrl.$inject = ['$scope', '$http'];

function AppCtrl($scope, $http) {
    var vm = this;
    vm.fields = [
        {label: 'Name', key: 'name'},
        {label: 'Email', key: 'email'},
        {label: 'Phone', key: 'phone'}
    ];
    vm.record = {};
    vm.records = [];
    $scope.currentPage = 1;
    $scope.maxSize = 10; // max number of buttons
    
    vm.handleError = function(response) {
        console.log(response.status + " - " + response.statusText + " - " + response.data);
    }

    // Get records on current page
    vm.getRecords = function() {
        $http.get('/records?page=' + $scope.currentPage).then(function(response){
            vm.data = response.data;
        }, function(response){
            vm.handleError(response);
        });
    }

    // Update view when page changes
    $scope.pageChanged = function() {
        vm.getRecords();
    }

    vm.getRecords();

    vm.editMode = false;
    vm.saveRecord = function() {
        if(vm.editMode) {
            vm.updateRecord();
        } else {
            vm.addRecord();
        }
    }

    vm.addRecord = function() {
        console.log(vm.record);
        $http.post('/records', vm.record).then(function(response){
            vm.record = {};
            vm.getRecords();
        }, function(response){
            vm.handleError(response);
        });
    }

    vm.updateRecord = function() {
        $http.put('/records/' + vm.record._id, vm.record).then(function(response){
            vm.record = {};
            vm.getRecords();
            vm.editMode = false;
        }, function(response){
            vm.handleError(response);
        });
    }

    vm.editRecord = function(record) {
        vm.record = record;
        vm.editMode = true;
    }

    vm.deleteRecord = function(recordid) {
        $http.delete('/records/'+recordid).then(function(response){
            console.log("Deleted");
            vm.getRecords();
        }, function(response){
            vm.handleError(response);
        })
    }

    vm.cancelEdit = function() {
        vm.editMode = false;
        vm.record = {};
        vm.getRecords();
    }

}
