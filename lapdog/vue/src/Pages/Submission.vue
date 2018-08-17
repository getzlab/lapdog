<template lang="html">
  <div id="submission">
    <h4>
      <router-link :to="{name: 'workspace', params: {namespace: namespace, workspace: workspace}}">
        {{namespace}}/{{workspace}}
      </router-link>
    </h4>
    <h3>Submission</h3>
    <div class="row">
      <div class="col s2">
        Global ID:
      </div>
      <div class="col s10" v-if="submission">
        {{submission.identifier}}
      </div>
    </div>
    <div class="row">
      <div class="col s2">
        Local ID:
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
        {{submission.operation}}
      </div>
    </div>
    <div class="row">
      <div class="col s2">
        Status:
      </div>
      <div class="col s10 red-text">
        Stand-in [Running/Finished]
      </div>
    </div>
    <div class="row">
      <div class="col s2">
        Workflows:
      </div>
      <div class="col s10" v-if="submission">
        {{submission.workflows.length}}
      </div>
    </div>
    <div class="row">

    </div>
    <h4>Workflows</h4>
    <div class="row" v-if="submission">
      <div class="col s12">
        Waiting for {{submission.workflows.length}} workflows to check in...
      </div>
    </div>
  </div>
</template>

<script>
import axios from'axios'
import _ from 'lodash'
export default {
  props: ['namespace', 'workspace', 'submission_id'],
  data() {
    return {
      submission: null,
    }
  },
  created() {
    this.init(this.namespace, this.workspace, this.submission_id);
  },
  methods: {
    init(namespace, workspace, sid) {
      this.submission = null;
      axios.get('http://localhost:4201/api/v1/submissions/expanded/'+namespace+'/'+workspace+'/'+sid)
        .then(response => {
          console.log("Got submission");
          this.submission = response.data;
        })
        .catch(response => {
          console.error("Failed");
          console.error(response)
        })
    },
    beforeRouteUpdate(to, from, next) {
      this.init(to.params.namespace, to.params.workspace, to.params.submission_id)
      next();
    }
  }
}
</script>

<style lang="css">
</style>
