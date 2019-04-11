<template lang="html">
  <div class="row" v-if="n_pages > 1" style="margin-bottom: 0px;">
    <div class="col s12">
      <ul class="pagination center">
        <li v-bind:class="current_page == 0 ? 'disabled' : ''" v-on:click.prevent="turn_page(0)">
          <a href="#"><i class="material-icons">first_page</i></a>
        </li>
        <li v-bind:class="current_page == 0 ? 'disabled' : ''" v-on:click.prevent="turn_page(current_page - 1)">
          <a href="#"><i class="material-icons">chevron_left</i></a>
        </li>
        <li v-for="page in page_range" v-on:click.prevent="turn_page(page)" v-bind:class="page == current_page ? 'active' : ''">
          <a href="#">{{page + 1}}</a>
        </li>
        <li v-bind:class="current_page == n_pages - 1 ? 'disabled' : ''" v-on:click.prevent="turn_page(current_page + 1)">
          <a href="#"><i class="material-icons">chevron_right</i></a>
        </li>
        <li v-bind:class="current_page == n_pages - 1 ? 'disabled' : ''" v-on:click.prevent="turn_page(n_pages - 1)">
          <a href="#"><i class="material-icons">last_page</i></a>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
export default {
  name:'pagination',
  props: ['n_pages', 'page_size', 'getter'],
  data() {
    return {
      lodash: _,
      current_page: 0
    }
  },
  computed: {
    page_range() {
      return _.range(
        _.max([0, this.current_page - 5]),
        _.min([this.n_pages, this.current_page + 6])
      );
    }
  },
  methods: {
    turn_page(n) {
      this.current_page = n;
      this.getter(n);
    }
  }

}
</script>

<style lang="css" scoped>
  ul.pagination li.active {
    background: #1e88e5;
  }
</style>
