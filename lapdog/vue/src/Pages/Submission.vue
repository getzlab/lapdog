<!--
Icons:
Done-ok: check_cicle
Aborted: sync_disabled
Running: sync
Failed: sync_problem
Other: error_outline
-->
<template lang="html">
  <div id="submission">
    <div class="modal" id="rerun-modal">
      <div class="modal-content">
        Created rerun entity set <span v-if="rerun_set"><code>{{rerun_set.name}}</code></span>
        <br>Automatically rerun?
      </div>
      <div class="modal-footer">
        <div class="row">
          <div class="col s6 offset-s3">
            <a class="modal-close red-text left" v-on:click="submit_rerun">YES</a>
            <a class="modal-close right">NO</a>
          </div>
        </div>
      </div>
    </div>
    <div class="modal" id="abort-modal">
      <div class="modal-content">
        <h4>Abort Submission?</h4>
      </div>
      <div class="modal-footer">
        <div class="row">
          <div class="col s6 offset-s3">
            <a class="modal-close red-text left" v-on:click="abort_sub">YES</a>
            <a class="modal-close right">NO</a>
          </div>
        </div>
      </div>
    </div>
    <div class="modal" id="operation-modal">
      <div class="modal-content">
        <div class="containe">
          <h4>Operation</h4>
          <div class="log-container grey lighten-3" style="max-height: 80% !important;">
            {{'\n'+active_operation}}
          </div>
        </div>
      </div>
    </div>
    <div class="modal" id="log-modal">
      <div class="modal-content">
        <div class="containe">
          <h4>Log Text</h4>
          <div class="log-container grey lighten-3" style="max-height: 80% !important;">
            {{'\n'+active_log}}
          </div>
        </div>
      </div>
    </div>
    <div class="modal" id="workflow-modal">
      <div class="modal-content">
        <div class="container">
          <h4>Workflow Info</h4>
          <div class="workflow-data" v-if="active_workflow">
            <div class="row" >
              <div class="col s2">
                Workflow Entity:
              </div>
              <div class="col s10">
                {{active_workflow.entity}}
              </div>
            </div>
            <div class="row">
              <div class="col s2">
                Workflow Status:
              </div>
              <div class="col s10">
                {{active_workflow.status}}
              </div>
            </div>
            <div style="overflow-x: scroll;">
              <ul class="collapsible" v-if="active_workflow.inputs">
                <li>
                  <div class="collapsible-header">
                    <i class="material-icons">input</i>
                    Workflow Inputs
                  </div>
                  <div class="collapsible-body">
                    <table>
                      <thead>
                        <tr>
                          <th>Input</th>
                          <th>Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr v-for="attribute in lodash.keys(active_workflow.inputs)">
                          <td v-bind:title="attribute">{{attribute}}</td>
                          <td v-bind:title="active_workflow.inputs[attribute]">
                            <a v-if="lodash.startsWith(lodash.toString(active_workflow.inputs[attribute]), 'gs://')"
                              v-bind:href="'https://accounts.google.com/AccountChooser?continue=https://console.cloud.google.com/storage/browser/'+active_workflow.inputs[attribute].substr(5)"
                              target="_blank" rel="noopener"
                            >
                              {{active_workflow.inputs[attribute]}}
                            </a>
                            <span v-else>{{active_workflow.inputs[attribute]}}</span>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </li>
              </ul>
            </div>
            <div class="row">
              <!-- <div class="col s12">
                Calls
              </div> -->
              <h5>Tasks</h5>
            </div>
            <ul class="collapsible">
              <li v-for="call in active_workflow.calls">

                <div class="collapsible-header" v-bind:title="call.status">
                  <i v-if="call.status == 'Success'" class="material-icons">check_cicle</i>
                  <i v-else-if="call.status == 'Preempted' || call.status == 'Cancelled'" class="material-icons">sync_disabled</i>
                  <i v-else-if="call.status == 'Failed'" class="material-icons">sync_problem</i>
                  <i v-else-if="call.status == 'Running'" class="material-icons">sync</i>
                  <i v-else class="material-icons">error_outline</i>
                  {{call.task}}
                </div>
                <div class="collapsible-body">
                  <div class="row">
                    <div class="col s2">
                      Status:
                    </div>
                    <div class="col s10" v-bind:class="call.status == 'Done' ? 'green-text' : call.status == 'Failed' ? 'red-text' : 'black-text'">
                      {{call.status}}
                    </div>
                  </div>
                  <div class="row">
                    <div class="col s2">
                      Operation ID:
                    </div>
                    <div class="col s10 truncate">
                      <a href="#" v-on:click.prevent="get_operation(call.operation)">{{call.operation}}</a>
                    </div>
                  </div>
                  <div class="row">
                    <div class="col s2">
                      Google Bucket:
                    </div>
                    <div class="col s10 truncate">
                      <a target="_blank" v-bind:href="'https://accounts.google.com/AccountChooser?continue=https://console.cloud.google.com/storage/browser/'+call.gs_path.substring(5)">
                        {{call.gs_path}}
                      </a>
                    </div>
                  </div>
                  <div class="row">
                    <div class="col s2">
                      Runtime:
                    </div>
                    <div class="col s10">
                      {{call.runtime > 1 ? "" + Math.floor(call.runtime) + " Hours" : "" + Math.floor(call.runtime*60) + " Minutes"}}
                    </div>
                  </div>
                  <div class="row" v-if="call.message && call.message.length">
                    <div class="col s2">
                      Last Message:
                    </div>
                    <div class="col s10 truncate" v-bind:title="call.message">
                      {{call.message}}
                    </div>
                  </div>
                  <div class="row" v-if="call.code">
                    <div class="col s2">
                      Return Code:
                    </div>
                    <div class="col s10" v-bind:class="call.code == 0 ? 'green-text' : 'red-text'">
                      {{call.code}}
                    </div>
                  </div>
                  <div class="row">
                    <div class="col s2">
                      Logs:
                    </div>
                    <div class="col s3">
                      <a href="#" v-on:click.prevent="get_log('stdout', call.idx)">Standard Out</a>
                    </div>
                    <div class="col s3">
                      <a href="#" v-on:click.prevent="get_log('stderr', call.idx)">Standard Error</a>
                    </div>
                    <div class="col s3">
                      <a href="#" v-on:click.prevent="get_log('google', call.idx)">Cromwell</a>
                    </div>
                  </div>
                </div>
              </li>
            </ul>
          </div>
          <div v-else>
            <div class="progress">
              <div class="indeterminate blue"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <h4>
      <router-link :to="{name: 'workspace', params: {namespace: namespace, workspace: workspace}}">
        <i class="material-icons tiny">arrow_back</i>  {{namespace}}/{{workspace}}
      </router-link>
    </h4>
    <div class="submission-data-container">
      <h3>Submission</h3>
      <div class="row">
        <div class="col s2">
          Submission ID:
        </div>
        <div class="col s10" v-if="submission">
          {{submission.identifier}}
        </div>
        <div class="col s10" v-else>
          Loading...
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Local Submission ID:
        </div>
        <div class="col s6">
          {{submission_id}}
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Operation ID:
        </div>
        <div class="col s10" v-if="submission">
          <a href="#" v-on:click.prevent="get_operation(submission.operation)">{{submission.operation}}</a>
        </div>
        <div class="col s10" v-else>
          Loading...
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Google bucket:
        </div>
        <div class="col s10" v-if="submission">
          <preview v-bind:value="submission.gs_path"></preview>
        </div>
        <div class="col s10" v-else>
          Loading...
        </div>
      </div>
      <div v-if="submission">
        <div class="row">
          <div class="col s2">
            Submission Date:
          </div>
          <div class="col s3">
            {{submission.submissionDate}}
          </div>
          <div class="col s5" v-if="cost">
            (Running for {{Math.round(cost.clock_h * 10) / 10}} hours)
          </div>
        </div>
        <div class="row">
          <div class="col s2">
            Configuration:
          </div>
          <div class="col s4">
            <router-link :to="{ name: 'methods', params: {namespace:namespace, workspace:workspace, target_namespace: submission.methodConfigurationNamespace, target_name: submission.methodConfigurationName} }">
              {{submission.methodConfigurationNamespace+'/'+submission.methodConfigurationName}}
            </router-link>
          </div>
          <div class="col s1">
            Entity:
          </div>
          <div class="col s5">
            {{submission.submissionEntity.entityName}}
          </div>
        </div>
      </div>
      <div class="row">
        <div class="col s1">
          Status:
        </div>
        <div class="col s3" v-if="submission"
          v-bind:class="submission.status == 'Failed' || submission.status == 'Error' ? 'red-text' : (submission.status == 'Succeeded' ? 'green-text' : '')">
          {{submission.status}}
        </div>
        <div class="col s3" v-else>
          Loading...
        </div>
        <div class="col s1">
          Cost:
        </div>
        <div class="col s3" v-if="cost">
          ${{cost.est_cost}}
          <span v-if="cost.cromwell_overhead" class="tooltipped" data-tooltip="Cromwell Overhead + Compute Cost">(${{cost.cromwell_overhead}} + ${{Math.floor((cost.est_cost - cost.cromwell_overhead) * 100)/100}})</span>
        </div>
        <div class="col s3" v-else>
          Loading...
        </div>
        <div class="col s1">
          Workflows:
        </div>
        <div class="col s3" v-if="submission">
          {{submission.workflows.length}}
        </div>
        <div class="col s3" v-else>
          Loading...
        </div>
      </div>
      <div class="row" v-if="submission">
        <div class="col s6" v-if="submission.status == 'Succeeded' || submission.status == 'Failed'">
          <a class='btn blue' v-on:click.prevent="upload">Upload Results</a>
        </div>
        <div class="col s6" v-if="(submission.status == 'Error' || submission.status == 'Failed') && workflows && failed_workflows.length">
          <button class='btn blue' data-target="rerun-modal" v-on:click="rerun">Rerun Failures</button>
        </div>
        <div class="col s6" v-if="submission.status == 'Running' || submission.status == 'Aborting'">
          <button class='btn red darken-3 modal-trigger' data-target="abort-modal">Abort Submission</button>
        </div>
      </div>
      <div class="row">
        <div v-if="!display_cromwell" class="col s12 expandable" v-on:click.prevent="read_cromwell">
          <span>
            <i class="material-icons">keyboard_arrow_right</i>
            Cromwell Log
          </span>
        </div>
        <div v-else class="col s12 expandable" v-on:click.prevent="display_cromwell = false">
          <span>
            <i class="material-icons">keyboard_arrow_down</i>
            Cromwell Log
          </span>
        </div>
      </div>
      <div v-if="display_cromwell">
        <div v-if="cromwell_lines" style="border: 1px solid grey;">
        <!-- <div v-if="cromwell_lines" class="card-panel"> -->
          <!-- <p class="flow-text">{{'...\n'+cromwell_lines.join('\n')}}</p> -->
          <div class="log-container grey lighten-3" id="cromwell-log">
            {{'\n'+cromwell_lines.join('\n')}}
          </div>
        </div>
        <div v-else>
          <div class="progress">
            <div class="indeterminate blue"></div>
          </div>
        </div>
      </div>
    </div>
    <div class="row">

    </div>
    <h4>Workflows</h4>
    <div class="row" v-if="pending_workflows">
      <div class="col s12">
        <span v-if="workflows">Refresh the page to re-query remaining {{pending_workflows}} workflow(s)</span>
        <span v-else>Waiting for {{pending_workflows}} workflows to check in...</span>
      </div>
    </div>
    <div class="workflow-container" v-if="workflows && workflows.length">
      <pagination ref="pagination" v-bind:n_pages="lodash.ceil(workflows.length / 20)" v-bind:page_size="20" v-bind:getter="turn_page"></pagination>
      <table class="highlight">
        <thead>
          <tr>
            <th v-on:click="sort_workflows('entity')">
              <i v-if="sort_key == 'entity' && !reversed" class="material-icons tiny">
                keyboard_arrow_down
              </i>
              <i v-else-if="sort_key == 'entity'" class="material-icons tiny">
                keyboard_arrow_up
              </i>
              <span class="expandable">Entity</span>
            </th>
            <th v-on:click="sort_workflows('status')">
              <i v-if="sort_key == 'status' && !reversed" class="material-icons tiny">
                keyboard_arrow_down
              </i>
              <i v-else-if="sort_key == 'status'" class="material-icons tiny">
                keyboard_arrow_up
              </i>
              <span class="expandable">Status</span>
            </th>
            <th v-on:click="sort_workflows('id')">
              <i v-if="sort_key == 'id' && !reversed" class="material-icons tiny">
                keyboard_arrow_down
              </i>
              <i v-else-if="sort_key == 'id'" class="material-icons tiny">
                keyboard_arrow_up
              </i>
              <span class="expandable">Workflow ID</span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="workflow in visible_workflows">
            <td>{{workflow.entity}}</td>
            <td v-bind:class="workflow.status == 'Success' ? 'green-text' : (workflow.status == 'Failed' || workflow.status == 'Error')? 'red-text' : 'black-text'">
              {{workflow.status}}
            </td>
            <td>
              <a v-on:click.prevent="display_workflow(workflow.id)" href="#">{{workflow.id}}</a>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else-if="!workflows">
      <div class="progress">
        <div class="indeterminate blue"></div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from'axios';
import _ from 'lodash';
import timeago from 'timeago.js';
export default {
  props: ['namespace', 'workspace', 'submission_id'],
  data() {
    return {
      lodash: _,
      submission: null,
      workflows: null,
      display_cromwell: false,
      cromwell_offset: 0,
      cromwell_lines: null,
      active_workflow: null,
      active_operation: null,
      active_log: null,
      sort_key: null,
      reversed: false,
      cost: null,
      rerun_set: null,
      rerun_type: null,
      page: 0,
      cancel: null
    }
  },
  computed: {
    pending_workflows() {
      if (this.submission) {
        return this.workflows ? this.submission.workflows.length - this.workflows.length : this.submission.workflows.length;
      }
      return 0;
    },
    failed_workflows() {
      if (this.workflows) {
        return _.filter(this.workflows, (workflow) => {
          return workflow.status == 'Failed' || workflow.status == 'Error';
        });
      } else return null;
    },
    visible_workflows() {
      return _.slice(this.workflows, this.page * 20, (this.page + 1) * 20);
    }
  },
  created() {
    this.init(this.namespace, this.workspace, this.submission_id);
    window.$('.modal').modal();
  },
  watch: {
    submission_id(value) {
      console.log("Watch change!");
      this.init(this.namespace, this.workspace, this.submission_id)
    }
  },
  methods: {
    init(namespace, workspace, sid) {
      this.$emit('on-namespace-update', namespace);
      window.$('body').css('overflow', '');
      window.$('.modal').modal();
      this.submission = null;
      this.workflows = null;
      this.display_cromwell = false;
      this.cromwell_lines = null;
      this.active_workflow = null;
      this.active_operation = null;
      this.active_log = null;
      this.sort_key = null;
      this.reversed = false;
      this.cost = null;
      this.rerun_set = null;
      this.page = 0;
      if (this.cancel) this.cancel.cancel("Recycling submission page");
      this.cancel = axios.CancelToken.source();
      axios.get(API_URL+'/api/v1/submissions/expanded/'+namespace+'/'+workspace+'/'+sid, {cancelToken: this.cancel.token})
        .then(response => {
          console.log("Got submission");
          this.submission = response.data;
          console.log("Fetching workflows");
          axios.get(API_URL+'/api/v1/submissions/expanded/'+namespace+'/'+workspace+'/'+sid+'/workflows', {cancelToken: this.cancel.token})
            .then(response => {
              console.log("Got workflows");
              console.log(response.data);
              this.workflows = response.data
              setTimeout(() => {
                window.$('.collapsible').collapsible();
                window.$('#abort-modal').modal();
              }, 100);
            })
            .catch(error => {
              if(axios.isCancel(error)) return;
              console.error("Failed");
              console.error(error);
              window.materialize.toast({
                html: "Unable to load workflows"
              })
            })
            axios.get(API_URL+'/api/v1/submissions/expanded/'+namespace+'/'+workspace+'/'+sid+'/cost', {cancelToken: this.cancel.token})
              .then(response => {
                console.log("Got cost");
                console.log(response.data);
                this.cost = response.data;
                setTimeout(() => {
                  window.$('.tooltipped').tooltip();
                }, 100);
              })
              .catch(error => {
                if(axios.isCancel(error)) return;
                console.error("Failed");
                console.error(error);
                window.materialize.toast({
                  html: "Unable to load cost estimate"
                })
              })
        })
        .catch(response => {
          if(axios.isCancel(response)) return;
          console.error("Failed");
          console.error(response)
          console.log(response.response.data);
          if (_.has(response.response, 'data') && response.response.data == "No Such Submission")
            window.materialize.toast({
              html: "Submission not found",
              displayLength: 10000
            })
          else window.materialize.toast({
            html: "Unable to load the submission: "+response.response.data.detail,
            displayLength: 10000
          })
        })
    },
    turn_page(page) {
      this.page = page;
    },
    sort_workflows(key) {
      if (key == this.sort_key) {
        this.workflows = _.reverse(this.workflows);
        this.reversed = !this.reversed;
      }
      else {
        this.reversed = false;
        this.sort_key = key;
        this.workflows = _.sortBy(this.workflows, key);
      }
      this.$refs.pagination.turn_page(0);
    },
    read_cromwell() {
      this.display_cromwell = true;
      console.log("Reading cromwell");
      axios.get(
        API_URL+'/api/v1/submissions/expanded/'+this.namespace+'/'+this.workspace+'/'+this.submission_id+'/cromwell',
        {
          cancelToken: this.cancel.token,
          params: {
            offset: this.cromwell_offset
          }
        }
      )
        .then(response => {
          console.log("Read cromwell lines");
          console.log(response.data);
          this.cromwell_lines = response.data;
          setTimeout(() => {
            let elem = window.$('#cromwell-log').get(0);
            elem.scrollTop = elem.scrollHeight;
          }, 100);
        })
        .catch(error => {
          if(axios.isCancel(error)) return;
          console.error("Failed");
          console.error(error);
          window.materialize.toast({
            html: "Unable to fetch Cromwell Log"
          });
        })
    },
    get_operation(operation_id) {
      this.active_operation = null;
      window.materialize.toast({
        html: "Checking operation "+operation_id+"...",
        displayLength: 2000,
      });
      axios.get(
        API_URL+'/api/v1/operations',
        {
          cancelToken: this.cancel.token,
          params: {operation_id: operation_id}
        }
      )
        .then(response => {
          this.active_operation = response.data;
          window.$('#operation-modal').modal();
          window.$('#operation-modal').modal('open');
        })
        .catch(error => {
          if(axios.isCancel(error)) return;
          console.error("FAILURE");
          console.error(error);
          window.materialize.toast({
            html: "Failed to get operation"
          });
        })
    },
    get_log(log_type, call_index) {
      this.active_log = null;
      window.materialize.toast({
        html: "Reading log...",
        displayLength: 2000,
      });
      axios.get(API_URL+'/api/v1/submissions/expanded/'+this.namespace+'/'+this.workspace+'/'+this.submission_id+'/workflows/'+this.active_workflow.id+'/'+log_type+'/'+call_index, {cancelToken: this.cancel.token})
        .then(response => {
          this.active_log = response.data;
          window.$('#log-modal').modal();
          window.$('#log-modal').modal('open');
        })
        .catch(error => {
          if(axios.isCancel(error)) return;
          console.error("FAILURE");
          console.error(error);
          window.materialize.toast({
            html: "Unable to access log. File not found",
            displayLength: 2000,
          });
        })
    },
    display_workflow(workflow_id) {
      this.active_workflow = null;
      window.materialize.toast({
        html: "Loading workflow "+workflow_id+"...",
        displayLength: 2000,
      });
      axios.get(API_URL+'/api/v1/submissions/expanded/'+this.namespace+'/'+this.workspace+'/'+this.submission_id+'/workflows/'+workflow_id, {cancelToken: this.cancel.token})
        .then(response => {
          console.log("Got workflow detail");
          console.log(response.data);
          this.active_workflow = response.data
          setTimeout(() => {
            window.$('.collapsible').collapsible();
          }, 100);
          window.$('#workflow-modal').modal();
          window.$('#workflow-modal').modal('open');
        })
        .catch(error => {
          if(axios.isCancel(error)) return;
          console.error("Failed");
          console.error(error);
          window.materialize.toast({
            html: "Failed to get workflow status"
          });
        })
    },
    abort_sub() {
      window.materialize.toast({
        html: "Aborting Submission",
        displayLength: 2000,
      });
      axios.delete(API_URL+'/api/v1/submissions/expanded/'+this.namespace+'/'+this.workspace+'/'+this.submission_id, {cancelToken: this.cancel.token})
        .then(response => {
          console.log("Abort results");
          console.log(response);
          window.materialize.toast({
            html: "Sent abort signal to submission",
            displayLength: 5000,
          });
          if (this.submission.status == 'Running') this.submission.status = 'Aborting';
          else this.submission.status = 'Aborted';
        })
        .catch(error => {
          if(axios.isCancel(error)) return;
          window.materialize.toast({
            html: "Failed to abort",
            displayLength: 10000,
          });
        })
    },
    upload() {
      window.materialize.toast({
        html: "Uploading results to Firecloud...",
        displayLength: 5000,
      });
      axios.put(API_URL+'/api/v1/submissions/expanded/'+this.namespace+'/'+this.workspace+'/'+this.submission_id, {}, {cancelToken: this.cancel.token})
        .then(response => {
          console.log("Upload results");
          console.log(response);
          if (!response.data.failed) {
            window.materialize.toast({
              html: "Success! Results have been saved to Firecloud",
              displayLength: 5000,
            });
          } else {
            window.materialize.toast({
              html: "Unable to upload results: " + response.data.message,
              displayLength: 10000,
            });
          }
        })
        .catch(error => {
          if(axios.isCancel(error)) return;
          window.materialize.toast({
            html: "Unable to upload results: " + response.data.message,
            displayLength: 10000,
          });
        })
    },
    rerun() {
      window.materialize.toast({
        html: "Creating entity set for " + (this.failed_workflows.length) + " failed entitites",
        displayLength: 4000
      });
      axios.put(API_URL+'/api/v1/submissions/expanded/'+this.namespace+'/'+this.workspace+'/'+this.submission_id+'/rerun', {}, {cancelToken: this.cancel.token})
        .then(response => {
          console.log("Rerun set");
          console.log(response.data);
          this.rerun_set = response.data;
          window.$('#rerun-modal').modal();
          window.$('#rerun-modal').modal('open');
        })
        .catch(response => {
          if(axios.isCancel(response)) return;
          console.log("Failed to create rerun set");
          console.error(response);
          window.materialize.toast({
            html: "Unable to create rerun set"
          })
        })
    },
    submit_rerun() {
      let query = API_URL+'/api/v1/workspaces/'+this.namespace+'/'+this.workspace+"/execute";
      let params = {
        config: this.submission.methodConfigurationNamespace+'/'+this.submission.methodConfigurationName,
        entity: this.rerun_set.name
      }
      if (this.rerun_set.expression) params.expression = this.rerun_set.expression;
      if (this.rerun_set.type) params.etype = this.rerun_set.type;
      if (this.submission.runtime && this.submission.runtime.private_access) params.private = true;
      if (this.submission.runtime && this.submission.runtime.region) params.region = this.submission.runtime.region;
      window.materialize.toast({
        html: "Preparing job...",
        displayLength: 10000,
      })
      axios.post(
        query,
        {},
        {
          cancelToken: this.cancel.token,
          params: params
        }
      )
        .then(response => {
          console.log("Execution returned");
          console.log(response);
          let result = response.data;
          if (result.ok && !result.failed) {
            window.materialize.toast({
              html: "It may take several minutes for the submission to check in",
              displayLength: 10000,
            })
            window.$('#rerun-modal').modal();
            window.$('#rerun-modal').modal('close');
            this.$router.push({
              name: 'submission',
              params: {
                namespace: this.namespace,
                workspace: this.workspace,
                submission_id: response.data.local_id
              }
            });
          }
          else {
            alert(response.data.message);
          }
        })
        .catch(response => {
          if(axios.isCancel(response)) return;
          console.error("FAILED");
          console.error(response);
          window.materialize.toast({
            html: "Failed to rerun submission. Try manually rerunning "+this.rerun_set.name
          })
        })
    }
  },
  beforeRouteUpdate(to, from, next) {
    console.log("Update!");
    this.init(to.params.namespace, to.params.workspace, to.params.submission_id)
    next();
  },
  beforeRouteLeave(to, from, next) {
    if (this.cancel) this.cancel.cancel("Switching routes");
    next();
  }
}
</script>

<style lang="css" scoped>
  div.log-container {
    max-height: 250px;
    border-radius: 8px;
    overflow-y: auto;
    /* margin: 1em;
    padding: 1em; */
    /* border: 1px solid black; */
    padding-left: 20px;
    font-family: monospace;
    white-space: pre-wrap;
    font-size: 90%;
  }

  table tr td {
    white-space: nowrap;
    text-overflow:ellipsis;
    overflow: hidden;
    max-width: 10px;
  }
</style>
