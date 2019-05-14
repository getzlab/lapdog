<template lang="html">
  <div id="workspace">
    <div class="modal" id="error-modal">
      <div class="modal-content">
        <h4>Failed to Submit</h4>
        <blockquote>
          {{submission_error}}
        </blockquote>
      </div>
    </div>
    <div class="modal" id="submission-modal">
      <div class="modal-content">
        <div class="container form-container">
          <h4>Execute Workflows</h4>
          <div class="row">
            <div class="input-field col s12">
              <select class="browser-default" id="config-select" v-model="submission_config" v-on:input="preflight(self_ref)">
                <option value="" selected disabled>Choose a method</option>
                <option v-for="config in method_configs" v-bind:value="config.namespace+'/'+config.name" :key="config.namespace+'/'+config.name">
                  {{config.namespace+'/'+config.name}}
                </option>
              </select>
            </div>
          </div>
          <div class="row">
            <div class="input-field col s6 inline">
              <select class="browser-default" id="etype-select" v-model="submission_etype" v-on:input="preflight(self_ref)">
                <option value="" selected disabled>Choose an entity type</option>
                <option v-for="etype in entity_types" v-bind:value="etype.type" >
                  {{etype.type}}
                </option>
              </select>
            </div>
            <div class="input-field col s6">
              <input autocomplete="off" class="autocomplete" type="text" id="entity-input" required v-model="entity_field" v-on:input="update_and_preflight" v-on:click="bind_autocomplete_listener"/>
              <label style="z-index: -1;">Workflow Entity</label>
            </div>
          </div>
          <div class="row">
            <div class="input-field col s12">
              <input autocomplete="off" type="text" id="expression-input" v-model="submission_expression" v-bind:disabled="extract_etype(submission_config) == submission_etype" v-on:input="preflight(self_ref)"/>
              <label style="z-index: -1;">Entity Expression (optional)</label>
            </div>
          </div>
          <div v-if="submission_message != ''" class="row">
            <div class="col s12" v-if="submission_message == '-'">
              <div class="preloader-wrapper small active">
                <div class="spinner-layer spinner-blue-only">
                  <div class="circle-clipper left">
                    <div class="circle"></div>
                  </div><div class="gap-patch">
                    <div class="circle"></div>
                  </div><div class="circle-clipper right">
                    <div class="circle"></div>
                  </div>
                </div>
              </div>
              Validating inputs...
            </div>
            <div v-else class="col s12" v-bind:class="submit_okay ? 'green-text' : 'red-text'">
              {{(submit_okay ? "" : "Error: ") + submission_message}}
            </div>
          </div>
          <div class="row" v-if="preflight_entities > batch_size">
            <div class="col s12 orange-text">
              Warning: This submission contains a large number of workflows.
              Unless otherwise specified, Cromwell will only run {{batch_size}} workflows at
              a time
            </div>
          </div>
          <div class="row expandable" v-on:click.prevent="toggle_advanced">
            <div class="col s12">
              <i class="material-icons">
                {{show_advanced ? "keyboard_arrow_down" : "keyboard_arrow_right"}}
              </i>
              Advanced Options
            </div>
          </div>
          <div v-if="show_advanced">
            <div class="row">
              <div class="col s3">
                Internet Access
              </div>
              <div class="col s9">
                <div class="switch">
                  <label>
                    Unrestricted
                    <input checked type="checkbox" v-model="private_access">
                    <span class="lever"></span>
                    Google Services Only (Recommended)
                  </label>
                </div>
              </div>
            </div>
            <div class="row">
              <div class="col s3">
                Compute Region
              </div>
              <div class="col s9 input-field" style="margin-top: 0px !important;">
                <select class="browser-default" id="region-select" v-model="compute_region">
                  <option v-bind:value="gateway.compute_regions[0]" selected>{{gateway.compute_regions[0]}} (default)</option>
                  <option v-for="region in lodash.slice(gateway.compute_regions, 1)" v-bind:value="region" :key="region">
                    {{region}}
                  </option>
                </select>
                <!-- <label for="region-select">Compute Region</label> -->
              </div>
            </div>
            <div class="row">
              <div class="col s3">
                Cromwell Memory
              </div>
              <div class="col s7">
                <input v-model="cromwell_mem" class="blue-text" type="range" name="cromwell-mem" min="3" max="624" step="1" value="3" v-on:change="batch_size = cromwell_mem == 3 ? 250 : Math.floor(0.75 * cromwell_mem * 250/3)">
              </div>
              <div class="col s2">
                {{cromwell_mem}} GB ${{
                  //Math.floor((cromwell_mem == 3 ? 0.05 : 0.063222 + (0.004237 * lodash.min([13, cromwell_mem]) ) + (cromwell_mem > 13 ? 0.009550 * (cromwell_mem - 13): 0))*100)/100
                  cromwell_mem == 3 ? 0.05 : Math.floor((
                    (Math.ceil(cromwell_mem/13) * 0.066348) + //core cost
                    (0.004446 * cromwell_mem) //mem cost
                  )*100)/100
                }}/hr
              </div>
            </div>
            <div class="row">
              <div class="col s3">
                Max Concurrent Workflows
              </div>
              <div class="col s7">
                <input v-model="batch_size" class="blue-text" type="range" name="cromwell-mem" min="100" v-bind:max="Math.floor(cromwell_mem * 250/3)" step="1" value="250">
              </div>
              <div class="col s2">
                {{batch_size}} Jobs
              </div>
            </div>
            <div class="row">
              <div class="col s3">
                Workflow Dispatch Rate
              </div>
              <div class="col s7">
                <input v-model="query_size" class="blue-text" type="range" name="cromwell-mem" min="1" v-bind:max="batch_size" step="1" value="250">
              </div>
              <div class="col s2">
                {{query_size}} Jobs/chunk
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <a class="btn-flat" v-bind:class="submit_okay ? '' : 'disabled'" v-on:click="submit_workflow">Run</a>
      </div>
    </div>
    <h4>{{namespace}}/{{workspace}}</h4>
    <div class="divider">

    </div>
    <div class="row">
      <div class="col s6">
        <h5><a rel="noopener" target="_blank" v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace">Visit Workspace in Firecloud</a></h5>
      </div>
      <div class="col s6">
        <h5>Lapdog Status</h5>
      </div>
    </div>
    <div v-if="workspace == 'do-not-delete-lapdog-resolution'" class="row">
      <div class="col s12 red-text">
        <h5>Resolution Workspace</h5>
      </div>
    </div>
    <div class="row">
      <div class="col s12" v-if="!ws">
        <div class="progress">
          <div class="indeterminate blue"></div>
        </div>
      </div>
    </div>
    <div class="stats-container" v-if="ws">
      <div class="row">
        <div class="col s2">
          Google Bucket:
        </div>
        <div class="col s4">
          <a target="_blank" v-bind:href="'https://accounts.google.com/AccountChooser?continue=https://console.cloud.google.com/storage/browser/'+ws.workspace.bucketName">
            {{ws.workspace.bucketName}}
          </a>
        </div>
        <div class="col s2">
          Execution Status:
        </div>
        <div class="col s4">
          <span v-if="!gateway" class="orange-text text-darken-4">
            Pending...
          </span>
          <span v-else-if="gateway.registered" class="green-text">
            Lapdog Engine Initialized
          </span>
          <span v-else-if="gateway.exists" class="red-text">
            Not Registered. Insufficient Permissions
          </span>
          <span v-else class="red-text">
            Not Ready. Contact Namespace Admin
          </span>
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Access Level:
        </div>
        <div class="col s4">
          {{ws.accessLevel}}
        </div>
        <div class="col s3">
          Workspace Cache State:
        </div>
        <div class="col s2 left tooltipped" v-bind:data-tooltip="syncing ? 'Syncing... Please Wait' : ('Pending Operations: ' + pending_ops)" data-position="right">
          <div class="switch">
            <label>
              Offline
              <input v-bind:disabled="syncing" v-bind:checked="cache_state" class="" type="checkbox" v-on:change="sync_cache">
              <span class="lever"></span>
              Online
            </label>
          </div>
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Owner{{ws.owners.length > 1 ? 's' : ''}}:
        </div>
        <div class="col s4">
          {{ws.owners.join(', ')}}
        </div>
        <div class="col s6">
          <router-link class="blue btn"
            :to="{name: 'methods', params: {namespace: namespace, workspace: workspace}}">
            View/Edit Method configurations
          </router-link>
        </div>
      </div>
      <div class="row">
        <div class="col s3">
          Active Firecloud Submissions:
        </div>
        <div class="col s3">
          <a v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace+'/monitor'" target="_blank" rel="noopener">
            {{ws.workspaceSubmissionStats.runningSubmissionsCount}}
          </a>
        </div>
        <div class="col s6 execute-container">
          <a href="#submission-modal" class='btn blue modal-trigger execute-button' v-on:click="activate_autocomplete" v-bind:class="gateway && gateway.registered && ws.entities && ws.entities.length ? '' : 'tooltipped disabled'"
            v-bind:data-tooltip="gateway && gateway.exists ? (ws.entities && ws.entities.length ? 'Ready' : 'There are no entities in this workspace') : 'Lapdog is not initialized for this namespace'"
          >
            Execute new job
          </a>
        </div>
      </div>
    </div>
    <div class="row expandable" v-on:click="show_attributes = !show_attributes" v-if="ws && ws.attributes && lodash.keys(ws.attributes).length">
      <div v-if="!show_attributes" class="col s6" >
        <span>
          <i class="material-icons">keyboard_arrow_right</i>
          Workspace Attributes
        </span>
      </div>
      <div v-else class="col s6">
        <span>
          <i class="material-icons">keyboard_arrow_down</i>
          Workspace Attributes
        </span>
      </div>
    </div>
    <div v-if="show_attributes">
      <!-- <div class="row">
        <div class="col s12">
          You can edit workspace attributes using the Lapdog command-line client,
          the Lapdog Python module, or on Firecloud
        </div>
      </div> -->
      <div class="row" >
        <div class="col s12 truncat">
          <table>
            <thead>
              <tr>
                <th>Attribute</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="key in lodash.keys(ws.attributes)" :key="key">
                <td>{{key}}</td>
                <td>
                  <!-- <a v-if="lodash.startsWith(lodash.toString(ws.attributes[key]), 'gs://')"
                    v-bind:href="'https://accounts.google.com/AccountChooser?continue=https://console.cloud.google.com/storage/browser/'+ws.attributes[key].substr(5)"
                    target="_blank" rel="noopener"
                  >
                    {{ws.attributes[key]}}
                  </a>
                  <span v-else>{{ws.attributes[key]}}</span> -->
                  <preview v-bind:value="ws.attributes[key]"></preview>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <br>
      <br>
    </div>
    <div class="submission-container">
      <div class="row">
        <div class="col s12">
          Lapdog Submissions:
        </div>
      </div>
      <!-- <div v-if="submissions" class="collection">
        <router-link v-for="sub in submissions" class="collection-item black-text"
          :to="{name: 'submission', params: {namespace:sub.namespace, workspace:sub.workspace, submission_id:sub.submission_id}}">
          <div>
            Ran <strong>{{sub.methodConfigurationName}}</strong> on <strong>{{sub.submissionEntity.entityName}}</strong> ({{sub.submission_id}})
          </div>
        </router-link>
      </div>
      <div v-else>
        <div class="progress">
          <div class="indeterminate blue"></div>
        </div>
      </div> -->
      <div v-if="cached_submissions || submissions == null">
        <div class="progress">
          <div class="indeterminate blue"></div>
        </div>
      </div>
      <div v-if="submissions && cached_submissions" class="row">
        <div class="col s12">
          Showing Cached Submissions:
        </div>
      </div>
      <div v-if="submissions" class="row">
        <pagination ref="submission_pagination" v-bind:n_pages="lodash.ceil(submissions.length / page_limit)" v-bind:page_size="page_limit" v-bind:getter="next_submissions"></pagination>
        <div class="col s12">
          <table class="highlight">
            <thead>
              <tr>
                <th>Configuration</th>
                <th>Entity</th>
                <th>Status</th>
                <th>Date</th>
                <th>Local Submission ID</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="sub in visible_submissions">
                <td>
                  <router-link :to="{ name: 'methods', params: {namespace:namespace, workspace:workspace, target_namespace:sub.methodConfigurationNamespace, target_name:sub.methodConfigurationName} }">
                    <!-- {{truncate_cell(sub.methodConfigurationNamespace+'/'+sub.methodConfigurationName, true)}} -->
                    {{sub.methodConfigurationNamespace+'/'+sub.methodConfigurationName}}
                  </router-link>
                </td>
                <td>{{sub.submissionEntity.entityName}}</td>
                <td v-bind:class="{Failed:'red-text', Error:'red-text', Aborted:'orange-text', Succeeded: 'green-text'}[sub.status]">
                  {{sub.status}}
                </td>
                <td>{{sub.submissionDate}}</td>
                <td>
                  <router-link :to="{name: 'submission', params: {namespace:sub.namespace, workspace:sub.workspace, submission_id:sub.submission_id}}">
                    {{sub.submission_id}}
                  </router-link>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="data-viewer" v-if="ws && ws.entities && ws.entities.length">
        <hr>
        <div class="row">
          <div class="col s12">
            <h4>
              <a rel="noopener" target="_blank" v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace+'/data'">Data</a>
            </h4>
          </div>
        </div>
        <div class="row">
          <div class="col s12">

            <!-- style="border-radius: 15px; border: 2px solid grey;" -->
            <ul class="pagination center" >
              <li v-for="etype in ws.entities" v-bind:class="active_entity == etype ? 'active' : ''"
                v-on:click.prevent="active_entity = etype"
              >
                <a href="#">{{etype.type}}s</a>
              </li>
            </ul>
            <hr>
          </div>
        </div>
        <div v-if="active_entity">
          <!-- <div class="row" v-if="active_entity.count > page_limit">
            <div class="col s12">
              <ul class="pagination center">
                <li v-bind:class="active_page == 0 ? 'disabled' : ''" v-on:click.prevent="turn_page(active_page - 1)">
                  <a href="#"><i class="material-icons">chevron_left</i></a>
                </li>
                <li v-for="page in page_range" v-on:click.prevent="turn_page(page)" v-bind:class="page == active_page ? 'active' : ''">
                  <a href="#">{{page + 1}}</a>
                </li>
                <li v-bind:class="active_page == max_pages.length - 1 ? 'disabled' : ''" v-on:click.prevent="turn_page(active_page + 1)">
                  <a href="#"><i class="material-icons">chevron_right</i></a>
                </li>
              </ul>
            </div>
          </div> -->
          <pagination v-bind:n_pages="max_pages.length" v-bind:page_size="page_limit" v-bind:getter="turn_page"></pagination>
          <!-- <div class="row" style="border: 1px solid grey;"> -->
          <div class="row z-depth-1">
            <div class="col s12" style="overflow-x: scroll;">
              <table v-if="entities_data">
                <thead>
                  <tr>
                    <th>{{active_entity.idName}}</th>
                    <th v-for="attr in active_entity.attributeNames">
                      {{attr}}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="entity in entities_data">
                    <td>{{entity[active_entity.idName]}}</td>
                    <td v-for="attr in active_entity.attributeNames" class="trunc">
                      <!-- <a v-if="lodash.startsWith(lodash.toString(entity[attr]), 'gs://')"
                        v-bind:href="'https://accounts.google.com/AccountChooser?continue=https://console.cloud.google.com/storage/browser/'+lodash.toString(entity[attr]).substr(5)"
                        target="_blank" rel="noopener"
                      >
                        {{truncate_cell(entity[attr])}}
                      </a>
                      <span v-else>
                        {{truncate_cell(entity[attr])}}
                      </span> -->
                      <preview v-bind:value="entity[attr]" v-bind:text="truncate_cell(entity[attr])"></preview>
                    </td>
                  </tr>
                </tbody>
              </table>
              <div v-else>
                <div class="progress">
                  <div class="indeterminate blue"></div>
                </div>
              </div>
            </div>
          </div>
          <!-- <div class="row" v-if="active_entity.count > page_limit">
            <div class="col s12">
              <ul class="pagination center">
                <li v-bind:class="active_page == 0 ? 'disabled' : ''" v-on:click.prevent="turn_page(active_page - 1)">
                  <a href="#"><i class="material-icons">chevron_left</i></a>
                </li>
                <li v-for="page in page_range" v-on:click.prevent="turn_page(page)" v-bind:class="page == active_page ? 'active' : ''">
                  <a href="#">{{page + 1}}</a>
                </li>
                <li v-bind:class="active_page == max_pages.length - 1 ? 'disabled' : ''" v-on:click.prevent="turn_page(active_page + 1)">
                  <a href="#"><i class="material-icons">chevron_right</i></a>
                </li>
              </ul>
            </div>
          </div> -->
        </div>
        <div v-else>
          <div class="row">
            <div class="col s12 center">
              Select an entity type above to view
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
  import axios from'axios'
  import _ from 'lodash'
  export default {
    props: ['namespace', 'workspace'],
    data() {
      return {
        lodash: _,
        ws: null,
        acl: null,
        cache_state: true,
        syncing: false,
        method_configs:null,
        entity_types: null,
        entity_field: "",
        submission_config: "",
        submission_etype: "",
        submission_expression: "",
        submission_message: "",
        submissions: null,
        expr_disabled: false,
        submit_okay: false,
        show_attributes: false,
        active_entity: null,
        entities_data: null,
        page_limit: 20,
        preflight_entities: 0,
        show_advanced: false,
        cromwell_mem: 3,
        batch_size: 250,
        query_size: 100,
        cached_submissions: true,
        gateway: null,
        pending_ops: 0,
        submission_error: null,
        private_access: true,
        compute_region: null,
        submission_page: 0,
        cancel: null
        // entities: null
      }
    },
    created() {
      this.getWorkspace(this.namespace, this.workspace);
      this.get_acl(this.namespace, this.workspace);
      // window.$('.modal').modal();
      // this.get_configs();
      // this.get_entities(this.namespace, this.workspace);
    },
    watch: {
      active_entity(newVal, oldVal) {
        if (!newVal) return;
        this.active_page = 0;
        window.materialize.toast({
          html: "Loading "+newVal.type+'s'
        });
        this.load_data(
          newVal.type,
          0,
          this.page_limit
        )
      },
    },
    computed: {
      self_ref() {
        return this;
      },
      max_pages() {
        return _.range(0, _.ceil(this.active_entity.count / this.page_limit));
      },
      visible_submissions() {
        return _.slice(this.submissions, this.submission_page * this.page_limit, (this.submission_page+1) * this.page_limit);
      },
      // active_entity: {
      //   get() {
      //     return this._active_entity ? this._active_entity : this.ws.entities[0]
      //   },
      //   set(etype) {
      //     window.materialize.toast({html:"New etype: "+etype.type})
      //     this._active_entity = etype;
      //     window.materialize.toast({html:"New etype: "+this._active_entity.type})
      //   }
      // }
    },
    methods: {
      // set_active(etype) {
      //   this.$set(this, '_active_entity', etype);
      // },
      toggle_advanced() {
        this.show_advanced = !this.show_advanced;
        setTimeout(
          () => {window.$('select').formSelect()},
          100
        );
      },
      turn_page(page)
      {
        if (page < 0 || page >= this.max_pages.length) return;
        this.active_page = page;
        this.load_data(
          this.active_entity.type,
          (this.active_page * this.page_limit),
          ((this.active_page+1) * this.page_limit)
        );
      },
      next_submissions(page) {
        this.submission_page = page;
      },
      load_data(etype, start, stop)
      {
        this.entities_data = null;
        axios.get(
          API_URL+'/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/entities/'+etype,
          {
            cancelToken:this.cancel.token,
            params: {
              'start': start,
              'end': stop
            }
          }
        )
          .then(response => {
            console.log("Obtained entities");
            this.entities_data = response.data;
          })
          .catch(error => {
            if(axios.isCancel(error)) return;
            console.error("Unable to load entities");
            console.error(error);
            window.materialize.toast({
              html: "Unable to load entities"
            })
          })
      },
      truncate_cell(value, front) {
        let string_value = _.toString(value);
        // return string_value;
        if (string_value.length > 25) {
          if (!front) string_value = string_value.substr(0, 25)+"...";//"..."+string_value.substr(string_value.length - 25);
          else string_value = "..."+string_value.substr(string_value.length - 25);
        }
        return string_value;
      },
      preflight: _.debounce((_this) => {
        _this.submission_message = "Incomplete fields";
        _this.preflight_entities = 0;
        if (_this.submission_config == "" || _this.submission_etype == "" || _this.entity_field == "") return;
        _this.submission_message = "-";
        console.log("Executing preflight");
        console.log(_this);
        let query = API_URL+'/api/v1/workspaces/'+_this.namespace+'/'+_this.workspace+"/preflight";
        let params = {
          config: _this.submission_config,
          entity: _this.entity_field
        }
        if (_this.submission_expression != "") params.expression = _this.submission_expression;
        if (_this.submission_etype != "") params.etype = _this.submission_etype;
        axios.post(
          query,
          {},
          {
            cancelToken:_this.cancel.token,
            params: params
          }
        )
          .then(response => {
            console.log("Preflight returned");
            console.log(response);
            let result = response.data;
            _this.submit_okay = result.ok && !result.failed;
            _this.preflight_entities = result.workflows;
            if (_this.submit_okay) _this.submission_message = "Ready to submit " + result.workflows + " workflow(s)";
            else _this.submission_message = result.message;
          })
          .catch(response => {
            if(axios.isCancel(response)) return;
            _this.submit_okay = false;
            _this.submission_message = "Unhandled error in response";
            console.error("FAILED");
            console.error(response)
            window.materialize.toast({
              html: "Encountered unexpected error while pre-validating submission"
            })
          })
      }, 500),
      submit_workflow() {
        this.submission_error = null;
        let query = API_URL+'/api/v1/workspaces/'+this.namespace+'/'+this.workspace+"/execute";
        let params = {
          config: this.submission_config,
          entity: this.entity_field,
          memory: this.cromwell_mem,
          batch: this.batch_size,
          query: this.query_size,
          private: this.private_access,
          region: this.compute_region
        }
        if (this.submission_expression != "") params.expression = this.submission_expression;
        if (this.submission_etype != "") params.etype = this.submission_etype;

        if (this.preflight_entities > 500) window.materialize.toast({
          html: "Preparing job. This may take a while...",
          displayLength: 10000,
        })
        else window.materialize.toast({
          html: "Preparing job...",
          displayLength: 10000,
        })
        axios.post(
          query,
          {},
          {
            cancelToken:this.cancel.token,
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
              window.$('#submission-modal').modal();
              window.$('#submission-modal').modal('close');
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
              this.submission_error = response.data.message; //_.replace(response.data.message, '\\n', '\n');
              window.$("#error-modal").modal();
              window.$("#error-modal").modal('open');
            }
          })
          .catch(response => {
            if(axios.isCancel(error)) return;
            console.error("FAILED");
            console.error(response)
            window.materialize.toast({
              html: "Unhandled exception while starting submission"
            });
          });
      },
      update_expr_mode() {
        this.expr_disabled = this.extract_etype(this.submission_config) == this.submission_etype;
        console.log(this.expr_disabled);
      },
      etype_crosscheck(cfg) {
        if(this.ws) {
          for(let config in this.ws.configs)
          {
            if(cfg == (config.namespace+'/'+config.name)) {
              console.log("Etype crosscheck: " +this.submission_etype == config.rootEntityType);
              return this.submission_etype == config.rootEntityType;
            }
          }
        }
        return false
      },
      extract_etype(cfg) {
        if(this.ws) {
          for(let config in this.ws.configs)
          {
            if(cfg == (this.ws.configs[config].namespace + '/'+this.ws.configs[config].name)) {
              return this.ws.configs[config].rootEntityType;
            }
          }
        }
        return false
      },
      validate_entity() {
        return this.entity_field == 'h' ? 'valid' : 'invalid';
      },
      getWorkspace(namespace, workspace) {
        this.show_attributes = false;
        this.active_entity = null;
        this.entities_data = null;
        this.show_advanced = false;
        this.cromwell_mem = 3;
        this.batch_size = 250;
        this.query_size = 100;
        this.cached_submissions = true;
        this.cache_state = true;
        this.syncing = false;
        this.gateway = null;
        this.pending_ops = 0;
        this.submission_error = null;
        this.private_access = true;
        this.submission_page = 0;
        if (this.cancel) this.cancel.cancel('Loading new workspace');
        this.cancel = axios.CancelToken.source();
        window.$('.tooltipped').tooltip();
        try {
          window.$('.execute-button').tooltip('destroy');
        } catch(error) {
          console.log(error);
        }
        // window.$('.execute-button').tooltip('close');
        this.$emit('on-namespace-update', namespace);
        axios.get(API_URL+'/api/v1/workspaces/'+namespace+'/'+workspace, {cancelToken:this.cancel.token})
          .then(response => {
            console.log(response.data);
            this.ws = response.data;
            this.entity_types = response.data.entities;
            this.method_configs = response.data.configs;
            window.$('.modal').modal();
            setTimeout(() => {
              window.$('.tooltipped').tooltip();
              console.log(this.ws.entities);
              console.log(window.$('.execute-button'));
              if (!(this.ws.entities && this.ws.entities.length)) window.$('.execute-container').hover(
                () => {
                  window.$('.execute-button').tooltip('open');
                },
                () => {
                  setTimeout(() => {
                    window.$('.execute-button').tooltip('close');
                  }, 100);
                }
              );
              else try {
                window.$('.execute-button').tooltip('destroy');
              } catch(error) {
                console.log(error);
              }
            }, 100);
            axios.get(API_URL+'/api/v1/workspaces/'+namespace+'/'+workspace+'/cache', {cancelToken:this.cancel.token})
              .then(response => {
                console.log(response.data);
                this.cache_state = response.data.state;
                this.pending_ops = response.data.pending;
                console.log("Fetching submissions");
                  axios.get(
                    API_URL+'/api/v1/workspaces/'+namespace+'/'+workspace+'/submissions',
                    {
                      cancelToken:this.cancel.token,
                      params: {cache: true}
                    }
                  )
                    .then(response => {
                      console.log("Fetched submissions");
                      console.log(response.data);
                      this.submissions = response.data;
                      axios.get(
                        API_URL+'/api/v1/workspaces/'+namespace+'/'+workspace+'/submissions',
                        {
                          cancelToken:this.cancel.token,
                          params: {cache: false}
                        }
                      )
                        .then(response => {
                          console.log("Fetched submissions");
                          console.log(response.data);
                          this.submissions = response.data;
                          this.cached_submissions = false;
                          if (this.$refs.submission_pagination) this.$refs.submission_pagination.turn_page(0);
                        })
                        .catch(error => {
                          if(axios.isCancel(error)) return;
                          console.error("FAIL");
                          console.error(error);
                          window.materialize.toast({
                            html: "Failed to query live submissions"
                          })
                        })
                    })
                    .catch(error => {
                      if(axios.isCancel(error)) return;
                      console.error("FAIL");
                      console.error(error);
                      window.materialize.toast({
                        html: "Failed to query cached submissions"
                      })
                    })
              })
              .catch(error => {
                if(axios.isCancel(error)) return;
                console.error("FAIL");
                console.error(error);
                window.materialize.toast({
                  html: "Unable to check workspace cache state"
                })
              })
          })
          .catch(error => {
            if(axios.isCancel(error)) return;
            console.error("FAIL");
            console.error(error);
            window.materialize.toast({
              html: "Failed to get workspace information"
            })
          })
      },
      activate_autocomplete() {
        setTimeout(
          () => {window.$('input.autocomplete').autocomplete({
            limit: 6
          })},
          250
        );
      },
      bind_autocomplete_listener() {
        setTimeout(
          () => {
            window.$("ul.autocomplete-content li").on(
              'click',
              (evt) => {
                console.log("Updating entitiy field");
                this.entity_field = evt.currentTarget.innerText;
              }
            );
          },
          100
        );
      },
      update_and_preflight() {
        this.update_autocomplete(this);
        this.preflight(this);
      },
      update_autocomplete: _.debounce((_this) => {
        if (_this.entity_field.length == 0) return;
        axios.get(API_URL+'/api/v1/workspaces/'+_this.namespace+'/'+_this.workspace+'/autocomplete/'+encodeURIComponent(_this.entity_field), {cancelToken:_this.cancel.token})
          .then(response => {
            window.$('input.autocomplete').autocomplete(
              'updateData',
              _.reduce(
                response.data,
                (obj, key) => {obj[key] = null; return obj},
                {}
              )
            );
            setTimeout(
              () => {
                if (window.$('input.autocomplete').is(':focus')) {
                  window.$('input.autocomplete').autocomplete('open');
                  _this.bind_autocomplete_listener();
                }
              },
              50
            );
          })
      }, 500),
     get_acl(namespace, workspace) {
        axios.get(API_URL+'/api/v1/workspaces/'+namespace+'/'+workspace+'/gateway', {cancelToken:this.cancel.token})
          .then(response => {
            console.log("RESPONSE");
            console.log(response.data);
            if (response.data.exists && !response.data.registered) {
              this.set_acl(this.namespace, this.workspace);
            }
            else {
              this.gateway = response.data;
              this.compute_region = this.gateway.compute_regions[0];
            }
            if (!this.gateway.exists) {
              window.materialize.toast({html: "Lapdog is not initialized for this namespace. Please contact an administrator", displayLength: 20000});
              window.$('.tooltipped').tooltip();
              window.$('.execute-container').hover(
                () => {
                  window.$('.execute-button').tooltip('open');
                },
                () => {
                  setTimeout(() => {
                    window.$('.execute-button').tooltip('close');
                  }, 100);
                }
              );
            }
          })
          .catch(error => {
            if(axios.isCancel(error)) return;
            console.error("FAIL")
            console.error(error);
            window.materialize.toast({
              html: "Unable to query gateway status"
            })
          })
      },
      set_acl(namespace, workspace) {
        console.log("UPDATING ACL");
        this.acl = null;
         axios.post(API_URL+'/api/v1/workspaces/'+namespace+'/'+workspace+'/gateway', {},{cancelToken:this.cancel.token})
           .then(response => {
             console.log("RESPONSE");
             console.log(response.data);
             this.gateway = response.data
             this.compute_region = this.gateway.compute_regions[0];
           })
           .catch(error => {
             if(axios.isCancel(error)) return;
             console.error("FAIL")
             console.error(error);
             window.materialize.toast({
               html: "Failed to register with gateway"
             })
           })
       },

      // get_entities(namespace, workspace) {
      //   axios.get(API_URL+'/api/v1/workspaces/'+namespace+'/'+workspace+'/entities')
      //     .then(response => {
      //       this.entities = response.data.entity_types;
      //       for(let etype in this.entities.entity_types)
      //       {
      //         // this.get_cache_state(this.entities.entity_types[etype].type);
      //       }
      //     })
      //     .catch(error => {
      //       console.error("FAIL")
      //       console.error(response)
      //     })
      // },
      sync_cache() {
        this.syncing = true;
        this.cache_state = !this.cache_state;
        axios.put(API_URL+'/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/cache', {}, {cancelToken:this.cancel.token})
          .then(response => {
            console.log(response.data);
            this.cache_state = response.data.state;
            this.pending_ops = response.data.pending;
            this.syncing = false;
          })
          .catch(error => {
            console.error("FAIL")
            console.error(error)
            window.materialize.toast({html:"Failed to sync the workspace cache: "+error.response.data});
            this.syncing = false;
          })
      },
      get_configs() {
        axios.get(API_URL+'/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs')
          .then(response => {
            console.log(response.data);
            this.method_configs = response.data;
            window.$('.modal').modal();
            // this.$forceUpdate();
          })
          .catch(error => {
            if(axios.isCancel(error)) return;
            console.error("FAIL");
            console.error(error);
            window.materialize.toast({
              html: "Failed to query available configurations"
            })
          })
      }
      // get_cache_state(etype) {
      //   axios.get(API_URL+'/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/cache/'+etype)
      //     .then(response => {
      //       console.log("Fetched cache state for "+etype);
      //       this.cache_state[etype] = response.data;
      //       console.log(this.cache_state);
      //     })
      //     .catch(error => {
      //       console.error("FAIL")
      //       console.error(response)
      //     })
      // }
    },
    beforeRouteUpdate(to, from, next) {
      this.ws = null;
      this.syncing = false;
      this.acl = null;
      this.entities = null;
      this.method_configs = null;
      this.submit_okay = false;
      this.submission_message = "";
      this.entity_field = "";
      this.submission_expression = "";
      this.submissions = null;
      this.active_entity = null;
      this.cached_submissions = true;
      this.cache_state = true;
      this.syncing = false;
      this.gateway = null;
      this.pending_ops = 0;
      this.submission_page = 0;
      this.getWorkspace(to.params.namespace, to.params.workspace);
      this.get_acl(to.params.namespace, to.params.workspace);
      // this.get_configs();
      next();
    },
    beforeRouteLeave(to, from, next) {
      if (this.cancel) this.cancel.cancel('Navigating to new route');
      next();
    }
  }
</script>

<style lang="css" scoped>

  div.data-viewer td {
    font-size: 0.9em;
    padding: 5px;
  }

  table tr td {
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
  }

  td.trunc .preview-display{
    max-width: 100px;
    direction: rtl;
  }

  .switch label input[type=checkbox]:checked+.lever {
    background-color: #1e88e5;
  }

  .switch label input[type=checkbox]:checked+.lever:after {
    background-color: #1e88e5;
  }

  input[type='range']::-webkit-slider-thumb {
      background-color: #1e88e5;
  }

  input[type='range']::-moz-range-thumb {
      background-color: #1e88e5;
  }

  input[type='range']::-ms-thumb {
      background-color: #1e88e5;
  }

  ul.pagination li.active {
    background: #1e88e5;
  }

</style>
